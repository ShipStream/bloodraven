package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	v1alpha1 "github.com/shipstream/bloodraven/api/v1alpha1"
	"github.com/shipstream/bloodraven/internal/license"
	"github.com/shipstream/bloodraven/internal/metrics"
)

const (
	eventLicenseInvalid  = "LicenseInvalid"
	eventLicenseVerified = "LicenseVerified"

	msgLicenseVerified      = "license verified"
	msgLicenseInvalid       = "license invalid"
	msgLicenseUpdatesEnded  = "license updates period ended"
	msgOperatorVerified     = "operator license verified"
	msgOperatorInvalid      = "operator license invalid"
	msgOperatorUpdatesEnded = "operator license updates period ended"
)

type licenseObservation struct {
	digest         string
	valid          string
	edition        string
	organization   string
	updatesExpired bool
	reason         string
	hasExpiry      bool
	expiryUnix     int64
}

func (r *MysqlFailoverGroupReconciler) licenseLogger() *slog.Logger {
	if r != nil && r.Logger != nil {
		return r.Logger
	}
	return slog.New(slog.DiscardHandler)
}

func (r *MysqlFailoverGroupReconciler) licenseKeys() license.KeyLookup {
	if r != nil && r.LicenseKeys != nil {
		return r.LicenseKeys
	}
	return license.ProductionKey
}

func (r *MysqlFailoverGroupReconciler) now() time.Time {
	if r != nil && r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

func (r *MysqlFailoverGroupReconciler) operatorLicense() string {
	if r == nil {
		return ""
	}
	return r.OperatorLicense
}

// ObserveLicense verifies the group's license offline and updates
// metrics/logs/events. It never returns an error and must not affect
// failover or reconcile results.
func (r *MysqlFailoverGroupReconciler) ObserveLicense(fg *v1alpha1.MysqlFailoverGroup) {
	r.observeLicense(fg)
}

func (r *MysqlFailoverGroupReconciler) observeLicense(fg *v1alpha1.MysqlFailoverGroup) {
	if r == nil || fg == nil {
		return
	}
	nn := types.NamespacedName{Namespace: fg.Namespace, Name: fg.Name}
	result := license.Resolve(fg.Spec.License, r.operatorLicense(), r.licenseKeys(), r.now())
	next := observationFromResult(fg.Spec.License, r.operatorLicense(), result)
	r.applyLicenseObservation(nn, fg, result, next)
}

func observationFromResult(mfgToken, operatorToken string, result license.Result) licenseObservation {
	valid := "false"
	if result.Valid {
		valid = "true"
	}
	org := result.Organization
	edition := result.Edition
	if edition == "" {
		edition = license.EditionCommunity
	}
	if !result.Valid {
		org = ""
		edition = license.EditionCommunity
	}
	obs := licenseObservation{
		digest:         licenseDigest(mfgToken, operatorToken, result),
		valid:          valid,
		edition:        edition,
		organization:   org,
		updatesExpired: result.UpdatesExpired,
		reason:         result.Reason,
	}
	if result.Valid && !result.Community && !result.UpdatesUntil.IsZero() {
		obs.hasExpiry = true
		obs.expiryUnix = result.UpdatesUntil.Unix()
	}
	return obs
}

func licenseDigest(mfgToken, operatorToken string, result license.Result) string {
	h := sha256.New()
	_, _ = h.Write([]byte(mfgToken))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(operatorToken))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(result.Source))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(result.Reason))
	_, _ = h.Write([]byte{0})
	if result.Valid {
		_, _ = h.Write([]byte("1"))
	} else {
		_, _ = h.Write([]byte("0"))
	}
	if result.UpdatesExpired {
		_, _ = h.Write([]byte("e"))
	}
	_, _ = h.Write([]byte(result.Organization))
	_, _ = h.Write([]byte(result.Edition))
	_, _ = h.Write([]byte(result.IssuedFor))
	_, _ = h.Write([]byte(result.Kid))
	return hex.EncodeToString(h.Sum(nil))
}

func (r *MysqlFailoverGroupReconciler) applyLicenseObservation(nn types.NamespacedName, fg *v1alpha1.MysqlFailoverGroup, result license.Result, next licenseObservation) {
	r.licenseMu.Lock()
	if r.licenseObs == nil {
		r.licenseObs = make(map[types.NamespacedName]licenseObservation)
	}
	prev, seen := r.licenseObs[nn]
	r.licenseObs[nn] = next
	r.licenseMu.Unlock()

	changed := !seen || prev.digest != next.digest

	// Write the current series first so a scrape cannot observe a gap,
	// then drop the previous label set only when it differs.
	metrics.SetLicenseInfo(nn.Namespace, nn.Name, next.organization, next.edition, next.valid)
	if next.hasExpiry {
		metrics.SetLicenseUpdatesExpiry(nn.Namespace, nn.Name, next.organization, next.edition, float64(next.expiryUnix))
	}
	if seen {
		sameInfo := prev.organization == next.organization && prev.edition == next.edition && prev.valid == next.valid
		if !sameInfo {
			metrics.DeleteLicenseInfo(nn.Namespace, nn.Name, prev.organization, prev.edition, prev.valid)
		}
		sameExpiry := prev.hasExpiry && next.hasExpiry &&
			prev.organization == next.organization && prev.edition == next.edition
		if prev.hasExpiry && !sameExpiry {
			metrics.DeleteLicenseUpdatesExpiry(nn.Namespace, nn.Name, prev.organization, prev.edition)
		}
	}

	if changed {
		r.emitLicenseSignals(fg, result, next, prev, seen)
	}
}

func (r *MysqlFailoverGroupReconciler) emitLicenseSignals(fg *v1alpha1.MysqlFailoverGroup, result license.Result, next, prev licenseObservation, seen bool) {
	logger := r.licenseLogger()
	fgName := fg.Namespace + "/" + fg.Name
	switch {
	case !result.Valid:
		logger.Warn(msgLicenseInvalid, "fg", fgName, "reason", result.Reason, "kid", result.Kid)
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeWarning, eventLicenseInvalid,
				"license token failed offline verification (%s); operator behavior is unchanged", result.Reason)
		}
	case result.Community:
		// Default free tier: no Event, no extra log on every group.
	case result.UpdatesExpired:
		logger.Info(msgLicenseUpdatesEnded,
			"fg", fgName,
			"organization", result.Organization,
			"edition", result.Edition,
			"updatesUntil", result.UpdatesUntil.UTC().Format(time.RFC3339),
		)
		if r.Recorder != nil && (!seen || !prev.updatesExpired || prev.digest != next.digest) {
			r.Recorder.Eventf(fg, corev1.EventTypeNormal, eventLicenseVerified,
				"license verified for %s (%s); update period ended %s; operator behavior is unchanged",
				result.Organization, result.Edition, result.UpdatesUntil.UTC().Format(time.RFC3339))
		}
	default:
		logger.Info(msgLicenseVerified,
			"fg", fgName,
			"organization", result.Organization,
			"edition", result.Edition,
			"updatesUntil", result.UpdatesUntil.UTC().Format(time.RFC3339),
			"issuedFor", result.IssuedFor,
			"kid", result.Kid,
		)
		if r.Recorder != nil {
			r.Recorder.Eventf(fg, corev1.EventTypeNormal, eventLicenseVerified,
				"license verified for %s (%s); updates until %s",
				result.Organization, result.Edition, result.UpdatesUntil.UTC().Format(time.RFC3339))
		}
	}
}

// forgetLicense drops metrics and observer state for a deleted group.
func (r *MysqlFailoverGroupReconciler) forgetLicense(nn types.NamespacedName) {
	if r == nil {
		return
	}
	r.licenseMu.Lock()
	prev, ok := r.licenseObs[nn]
	if ok {
		delete(r.licenseObs, nn)
	}
	r.licenseMu.Unlock()
	if ok {
		metrics.DeleteLicenseInfo(nn.Namespace, nn.Name, prev.organization, prev.edition, prev.valid)
		if prev.hasExpiry {
			metrics.DeleteLicenseUpdatesExpiry(nn.Namespace, nn.Name, prev.organization, prev.edition)
		}
	}
	metrics.DeleteLicense(nn.Namespace, nn.Name)
}

// LogOperatorLicense records the process-level operator default at startup.
// These messages have no fg field.
func LogOperatorLicense(logger *slog.Logger, token string, keys license.KeyLookup, now time.Time) {
	if logger == nil {
		return
	}
	token = strings.TrimSpace(token)
	if token == "" {
		return
	}
	if keys == nil {
		keys = license.ProductionKey
	}
	result := license.Verify(token, keys, now)
	switch {
	case !result.Valid:
		logger.Warn(msgOperatorInvalid, "reason", result.Reason, "kid", result.Kid)
	case result.UpdatesExpired:
		logger.Info(msgOperatorUpdatesEnded,
			"organization", result.Organization,
			"edition", result.Edition,
			"updatesUntil", result.UpdatesUntil.UTC().Format(time.RFC3339),
		)
	default:
		logger.Info(msgOperatorVerified,
			"organization", result.Organization,
			"edition", result.Edition,
			"updatesUntil", result.UpdatesUntil.UTC().Format(time.RFC3339),
			"issuedFor", result.IssuedFor,
			"kid", result.Kid,
		)
	}
}
