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
  end
end

if ready ~= nil and ready.observedGeneration ~= nil and obj.metadata.generation ~= nil then
  if ready.observedGeneration < obj.metadata.generation then
    hs.status = "Progressing"
    hs.message = "Controller processing updated spec"
    return hs
  end
end

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

if bootstrapping ~= nil and bootstrapping.status == "True" then
  if bootstrapping.reason == "Failed" then
    hs.status = "Degraded"
    hs.message = "Bootstrapping: " .. (bootstrapping.message or "Bootstrap failed")
  else
    hs.status = "Progressing"
    hs.message = bootstrapping.message or "Bootstrapping"
  end
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

if ready ~= nil and ready.status == "False" then
  hs.status = "Progressing"
  hs.message = ready.message or "Not ready"
  return hs
end

return hs
