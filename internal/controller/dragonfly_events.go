package controller

// Event reasons emitted by the Dragonfly subsystem. Stable strings —
// downstream filtering depends on them. See site/content/docs/8.observability/7.log-schema.md
// for the full reference.
const (
	ReasonDragonflyPromotionStarted         = "DragonflyPromotionStarted"
	ReasonDragonflyPromotionCompleted       = "DragonflyPromotionCompleted"
	ReasonDragonflyPromotionFailed          = "DragonflyPromotionFailed"
	ReasonDragonflyStaleMasterDetected      = "DragonflyStaleMasterDetected"
	ReasonDragonflySyncTimeout              = "DragonflySyncTimeout"
	ReasonDragonflyUpgradeStarted           = "DragonflyUpgradeStarted"
	ReasonDragonflyUpgradeRejected          = "DragonflyUpgradeRejected"
	ReasonDragonflyUpgradeSnapshotStarted   = "DragonflyUpgradeSnapshotStarted"
	ReasonDragonflyUpgradeSnapshotCompleted = "DragonflyUpgradeSnapshotCompleted"
	ReasonDragonflyUpgradeCompleted         = "DragonflyUpgradeCompleted"
	ReasonDragonflyUpgradeFailed            = "DragonflyUpgradeFailed"
	// ReasonDragonflyOldSiteReconfigured fires when DragonflyManager
	// auto-attaches a stale-master pod as a replica of the active
	// master after verifying it provably never accepted writes
	// (connected_slaves=0 && master_repl_offset=0).
	ReasonDragonflyOldSiteReconfigured = "DragonflyOldSiteReconfigured"
	// ReasonDragonflyReplTakeoverUnsupported fires when a capability
	// probe finds REPLTAKEOVER missing from the running command table.
	ReasonDragonflyReplTakeoverUnsupported = "DragonflyReplTakeoverUnsupported"
	// ReasonDragonflyReplTakeoverSupported fires on a false→true
	// capability transition (image upgrade restored the command).
	ReasonDragonflyReplTakeoverSupported = "DragonflyReplTakeoverSupported"
	// ReasonDragonflySessionsLost fires when emergency promotion
	// fell back to REPLICAOF NO ONE after REPLTAKEOVER failed.
	ReasonDragonflySessionsLost = "DragonflySessionsLost"
)
