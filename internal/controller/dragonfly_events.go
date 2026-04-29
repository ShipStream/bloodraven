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
	// ReasonDragonflyOldSiteReconfigured fires when DragonflyManager
	// auto-attaches a stale-master pod as a replica of the active
	// master after verifying it provably never accepted writes
	// (connected_slaves=0 && master_repl_offset=0).
	ReasonDragonflyOldSiteReconfigured = "DragonflyOldSiteReconfigured"
)
