package pitrarchive

import "strings"

// Token is a PITR refusal token (§7.1): the first word of a Job's
// termination message, of a sidecar LastError, or of a reconcile-time
// RestoreBuildFailed message. pitrFailureReason, MBV Verified=False reasons
// and status.restore.reason use the same strings. This is the one catalog;
// add a token here before emitting it anywhere.
type Token string

// Archive identity and store tokens.
const (
	TokenArchiveListFailed         Token = "PITRArchiveListFailed"
	TokenArchiveNotFound           Token = "PITRArchiveNotFound"
	TokenArchiveLegacy             Token = "PITRArchiveLegacy"
	TokenArchiveDescriptorMissing  Token = "PITRArchiveDescriptorMissing"
	TokenArchiveVersionUnsupported Token = "PITRArchiveVersionUnsupported"
	TokenArchiveIdentityMismatch   Token = "PITRArchiveIdentityMismatch"
	TokenArchiveForeign            Token = "PITRArchiveForeign"
	TokenArchiveCopy               Token = "PITRArchiveCopy"
	TokenArchiveEncryptionMismatch Token = "PITRArchiveEncryptionMismatch"
	TokenArchiveKeyMismatch        Token = "PITRArchiveKeyMismatch"
	TokenArchiveStoreUnsupported   Token = "PITRArchiveStoreUnsupported"
	TokenArchiveUnsafeHistory      Token = "PITRArchiveUnsafeHistory"
	TokenArchiveHistoryReset       Token = "PITRArchiveHistoryReset"
	TokenArchiveObjectInvalid      Token = "PITRArchiveObjectInvalid"
	TokenArchiveNamespaceReuse     Token = "PITRArchiveNamespaceReuse"
	TokenArchiveSplitBrain         Token = "PITRArchiveSplitBrain"
	TokenArchiveDivergentHistory   Token = "PITRArchiveDivergentHistory"
	TokenArchiveGap                Token = "PITRArchiveGap"
	TokenArchiveOrderConflict      Token = "PITRArchiveOrderConflict"
	TokenArchiveOrderInversion     Token = "PITRArchiveOrderInversion"
	TokenArchiveDestinationShared  Token = "PITRArchiveDestinationShared"
	TokenArchiveBindingNotReady    Token = "PITRArchiveBindingNotReady" // #203
	TokenBackupIdentityUnknown     Token = "PITRBackupIdentityUnknown"
	TokenBackupIdentityMismatch    Token = "PITRBackupIdentityMismatch"
	TokenBackupInconsistent        Token = "PITRBackupInconsistent"
	TokenBackupFiltered            Token = "PITRBackupFiltered"
	TokenBackupIncomplete          Token = "PITRBackupIncomplete"
	TokenBackupDumpKeyMissing      Token = "PITRBackupDumpKeyMissing" // #203
	TokenBackupAfterTarget         Token = "PITRBackupAfterTarget"
	TokenBackupNotOnLineage        Token = "PITRBackupNotOnLineage"
	TokenExcludeIntersectsBackup   Token = "PITRExcludeIntersectsBackup"
	TokenStopAmbiguous             Token = "PITRStopAmbiguous"
	TokenTargetNotCovered          Token = "PITRTargetNotCovered"
	TokenTargetInvalid             Token = "PITRTargetInvalid"
	TokenNoArchivedReplay          Token = "PITRNoArchivedReplay"
	TokenReplayUnsupported         Token = "PITRReplayUnsupported"
	TokenPlanTooLarge              Token = "PITRPlanTooLarge"
	TokenCoverageInvalid           Token = "PITRCoverageInvalid"
	TokenReplayMismatch            Token = "PITRReplayMismatch"
	// TokenPITRFailed is the replay script's failure sentinel. It stays in
	// Job messages and logs; the operator serves it as TokenReplayFailed.
	TokenPITRFailed Token = "BLOODRAVEN_PITR_FAILED"
)

// TokenReplayFailed is the reason the operator serves for TokenPITRFailed.
// It is not parsed out of messages, so it is not in AllTokens.
const TokenReplayFailed Token = "PITRReplayFailed"

// Served returns the reason the operator serves for t: t itself, except
// that the replay script's sentinel is served as TokenReplayFailed.
func (t Token) Served() Token {
	if t == TokenPITRFailed {
		return TokenReplayFailed
	}
	return t
}

// ServedTokens lists every reason the operator serves for the catalog:
// AllTokens, each through Served.
var ServedTokens = func() []Token {
	out := make([]Token, len(AllTokens))
	for i, t := range AllTokens {
		out[i] = t.Served()
	}
	return out
}()

// AllTokens lists every token in the catalog.
var AllTokens = []Token{
	TokenArchiveListFailed, TokenArchiveNotFound, TokenArchiveLegacy, TokenArchiveDescriptorMissing,
	TokenArchiveVersionUnsupported, TokenArchiveIdentityMismatch, TokenArchiveForeign, TokenArchiveCopy,
	TokenArchiveEncryptionMismatch, TokenArchiveKeyMismatch, TokenArchiveStoreUnsupported,
	TokenArchiveUnsafeHistory, TokenArchiveHistoryReset, TokenArchiveObjectInvalid,
	TokenArchiveNamespaceReuse, TokenArchiveSplitBrain, TokenArchiveDivergentHistory, TokenArchiveGap,
	TokenArchiveOrderConflict, TokenArchiveOrderInversion, TokenArchiveDestinationShared,
	TokenArchiveBindingNotReady, TokenBackupIdentityUnknown, TokenBackupIdentityMismatch,
	TokenBackupInconsistent, TokenBackupFiltered, TokenBackupIncomplete, TokenBackupDumpKeyMissing,
	TokenBackupAfterTarget, TokenBackupNotOnLineage, TokenExcludeIntersectsBackup, TokenStopAmbiguous,
	TokenTargetNotCovered, TokenTargetInvalid, TokenNoArchivedReplay, TokenReplayUnsupported,
	TokenPlanTooLarge, TokenCoverageInvalid, TokenReplayMismatch, TokenPITRFailed,
}

var tokenSet = func() map[Token]bool {
	m := make(map[Token]bool, len(AllTokens))
	for _, t := range AllTokens {
		m[t] = true
	}
	return m
}()

// Known reports whether t is in the catalog.
func (t Token) Known() bool { return tokenSet[t] }

// ParseToken returns the catalog token that starts msg (the first word,
// with a trailing ':' removed), or false.
func ParseToken(msg string) (Token, bool) {
	first, _, _ := strings.Cut(strings.TrimSpace(msg), " ")
	t := Token(strings.TrimSuffix(first, ":"))
	return t, t.Known()
}

// RefusalReason is a PITRArchiveReady=False reason (D-27): the token without
// its "PITR" prefix. The API mirrors it as v1alpha1.PITRArchiveRefusalReason.
type RefusalReason string

const (
	ReasonArchiveForeign            RefusalReason = "ArchiveForeign"
	ReasonArchiveCopy               RefusalReason = "ArchiveCopy"
	ReasonArchiveLegacy             RefusalReason = "ArchiveLegacy"
	ReasonArchiveDescriptorMissing  RefusalReason = "ArchiveDescriptorMissing"
	ReasonArchiveEncryptionMismatch RefusalReason = "ArchiveEncryptionMismatch"
	ReasonArchiveKeyMismatch        RefusalReason = "ArchiveKeyMismatch"
	ReasonArchiveVersionUnsupported RefusalReason = "ArchiveVersionUnsupported"
	ReasonArchiveStoreUnsupported   RefusalReason = "ArchiveStoreUnsupported"
	ReasonArchiveUnsafeHistory      RefusalReason = "ArchiveUnsafeHistory"
)

// ArchiveReadyReasons is the closed set of #206 PITRArchiveReady=False
// refusal reasons, in the order of the API enum.
var ArchiveReadyReasons = []RefusalReason{
	ReasonArchiveForeign, ReasonArchiveCopy, ReasonArchiveLegacy, ReasonArchiveDescriptorMissing,
	ReasonArchiveEncryptionMismatch, ReasonArchiveKeyMismatch, ReasonArchiveVersionUnsupported,
	ReasonArchiveStoreUnsupported, ReasonArchiveUnsafeHistory,
}

// Reason returns t's PITRArchiveReady reason, or false when t is not one of
// ArchiveReadyReasons.
func (t Token) Reason() (RefusalReason, bool) {
	r := RefusalReason(strings.TrimPrefix(string(t), "PITR"))
	for _, known := range ArchiveReadyReasons {
		if r == known {
			return r, true
		}
	}
	return "", false
}
