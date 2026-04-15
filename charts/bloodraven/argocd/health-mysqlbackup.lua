local hs = {}
hs.status = "Progressing"
hs.message = "Waiting for status"

if obj.status == nil then
  return hs
end

if obj.status.observedGeneration ~= nil and obj.metadata.generation ~= nil then
  if obj.status.observedGeneration < obj.metadata.generation then
    hs.status = "Progressing"
    hs.message = "Controller processing updated spec"
    return hs
  end
end

local phase = obj.status.phase
if phase == nil or phase == "" then
  return hs
end

if phase == "Succeeded" then
  hs.status = "Healthy"
  hs.message = obj.status.message or "Backup completed successfully"
elseif phase == "Failed" then
  hs.status = "Degraded"
  hs.message = obj.status.message or "Backup failed"
elseif phase == "Running" then
  hs.status = "Progressing"
  hs.message = obj.status.message or "Backup in progress"
elseif phase == "Pending" then
  hs.status = "Progressing"
  hs.message = obj.status.message or "Backup pending"
end

return hs
