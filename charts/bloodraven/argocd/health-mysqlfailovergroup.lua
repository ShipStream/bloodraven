local hs = {}
hs.status = "Progressing"
hs.message = "Waiting for status"

if obj.status == nil or obj.status.conditions == nil then
  return hs
end

local degraded = nil
local ready = nil
local bootstrapping = nil
local updating = nil
local recoveryPending = nil
local held = nil

for _, c in ipairs(obj.status.conditions) do
  if c.type == "Degraded" then
    degraded = c
  elseif c.type == "Ready" then
    ready = c
  elseif c.type == "Bootstrapping" then
    bootstrapping = c
  elseif c.type == "Updating" then
    updating = c
  elseif c.type == "RecoveryPending" then
    recoveryPending = c
  elseif c.type == "AutomaticFailoverHeld" then
    held = c
  end
end

-- A standing planned-failover automation hold needs an administrator:
-- automatic failover stays held until it is acknowledged.
if held ~= nil and held.status == "True" then
  hs.status = "Degraded"
  hs.message = "AutomaticFailoverHeld: " .. (held.message or "Automatic failover is held")
  return hs
end

-- Degraded covers a failed clone too (reason BootstrapFailed).
if degraded ~= nil and degraded.status == "True" then
  hs.status = "Degraded"
  hs.message = (degraded.reason or "Degraded") .. ": " .. (degraded.message or "")
  return hs
end

if recoveryPending ~= nil and recoveryPending.status == "True" then
  hs.status = "Degraded"
  hs.message = "RecoveryPending: " .. (recoveryPending.message or "Old primary recovery pending")
  return hs
end

if updating ~= nil and updating.status == "True" and updating.reason == "OrderedUpdateStalled" then
  hs.status = "Degraded"
  hs.message = "OrderedUpdateStalled: " .. (updating.message or "Ordered update stalled")
  return hs
end

-- status.observedGeneration is the generation the topology manager has
-- applied; every topology condition carries the same value. It can lag for
-- as long as no manager applies the spec (unreadable connection material,
-- a replacement manager that fails to start), so it gates only the
-- Progressing and Healthy verdicts below: a hold, Degraded, a pending
-- recovery or a stalled update stays visible meanwhile.
if obj.metadata.generation ~= nil then
  if obj.status.observedGeneration == nil or obj.status.observedGeneration < obj.metadata.generation then
    hs.message = "Controller processing updated spec"
    return hs
  end
end

-- Bootstrapping is True only while a clone runs.
if bootstrapping ~= nil and bootstrapping.status == "True" then
  hs.status = "Progressing"
  hs.message = bootstrapping.message or "Bootstrapping"
  return hs
end

if updating ~= nil and updating.status == "True" then
  hs.status = "Progressing"
  hs.message = updating.message or "Ordered update in progress"
  return hs
end

if ready ~= nil and ready.status == "True" then
  hs.status = "Healthy"
  hs.message = ready.message or "Ready"
  return hs
end

if ready ~= nil and (ready.status == "False" or ready.status == "Unknown") then
  hs.status = "Progressing"
  hs.message = ready.message or "Not ready"
  return hs
end

return hs
