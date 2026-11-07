package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// LastFailoverTime returns status.lastFailover.time, nil without a record.
func (s *MysqlFailoverGroupStatus) LastFailoverTime() *metav1.Time {
	if s.LastFailover == nil {
		return nil
	}
	return s.LastFailover.Time
}

// LastFailoverTarget returns status.lastFailover.target, "" without a
// record.
func (s *MysqlFailoverGroupStatus) LastFailoverTarget() string {
	if s.LastFailover == nil {
		return ""
	}
	return s.LastFailover.Target
}

// PromotionGtidExecuted returns status.lastFailover.promotionGtidExecuted,
// "" without a record.
func (s *MysqlFailoverGroupStatus) PromotionGtidExecuted() string {
	if s.LastFailover == nil {
		return ""
	}
	return s.LastFailover.PromotionGtidExecuted
}

// SetLastFailover replaces status.lastFailover. A record with no time, no
// target and no watermark is no record: it clears the field.
func (s *MysqlFailoverGroupStatus) SetLastFailover(t *metav1.Time, target, promotionGtidExecuted string) {
	if t == nil && target == "" && promotionGtidExecuted == "" {
		s.LastFailover = nil
		return
	}
	s.LastFailover = &FailoverRecord{
		FailoverRecordRef:     FailoverRecordRef{Time: t, Target: target},
		PromotionGtidExecuted: promotionGtidExecuted,
	}
}
