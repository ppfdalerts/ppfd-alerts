Set shell = CreateObject("Wscript.Shell")
cmd = """C:\WINDOWS\System32\WindowsPowerShell\v1.0\powershell.exe"" -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File ""C:\Program Files (x86)\PPFD-GroupMe-Package_20251115_223456\start_leaderboard.ps1"" -Headless -IntervalSec 30"
shell.Run cmd, 0, False
