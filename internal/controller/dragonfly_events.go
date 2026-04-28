package controller

// Event reasons emitted by the Dragonfly subsystem. Stable strings —
// downstream filtering depends on them. See docs/docs/log-schema.mdx
// for the full reference.
const (
	ReasonDragonflyPromotionStarted    = "DragonflyPromotionStarted"
	ReasonDragonflyPromotionCompleted  = "DragonflyPromotionCompleted"
	ReasonDragonflyPromotionFailed     = "DragonflyPromotionFailed"
	ReasonDragonflyStaleMasterDetected = "DragonflyStaleMasterDetected"
	ReasonDragonflySyncTimeout         = "DragonflySyncTimeout"
)
