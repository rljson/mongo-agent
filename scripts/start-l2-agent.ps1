# Starts the Laptop 2 mongo-agent over WinRM. Spawns a fully-detached
# process via Win32_Process.Create so it survives the WinRM session ending.
# Used by the dashboard's "Start L2 agent" button.
Invoke-Command -ComputerName 192.168.178.64 -ScriptBlock {
  $cmd = 'cmd.exe /c "cd /d C:\dev\mongo-agent && node --max-old-space-size=16384 --env-file=.env --import tsx/esm src\agent-server.ts > agent-l2.run.out 2> agent-l2.run.err"'
  $r = Invoke-CimMethod -ClassName Win32_Process -MethodName Create -Arguments @{ CommandLine = $cmd }
  Write-Output "PID=$($r.ProcessId)"
}
