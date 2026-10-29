package controller

import (
	"context"
	"errors"
	"time"

	"github.com/shipstream/bloodraven/internal/mysql"
)

const promotionBinlogRotationTimeout = time.Second

type promotionBinlogRotator interface{ FlushBinaryLogs(context.Context) error }

// Use a separate pooled connection so an optional rotation timeout cannot close
// the pinned connection carrying the maintenance handoff's promotion proof.
func (f *FailoverController) rotatePromotionBinlog(ctx context.Context, candidate mysql.Checker, site string) {
	rotationCtx, cancel := context.WithTimeout(ctx, promotionBinlogRotationTimeout)
	defer cancel()
	rotator, ok := candidate.(promotionBinlogRotator)
	var err error
	if ok {
		err = rotator.FlushBinaryLogs(rotationCtx)
	} else {
		err = errors.New("checker does not support binlog rotation")
	}
	if err != nil {
		f.logger.Warn("promotion binlog rotation failed", "site", site, "error", err)
	}
}
