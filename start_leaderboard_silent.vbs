Set shell = CreateObject("Wscript.Shell")
cmd = "powershell -NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File ""C:\Program Files (x86)\PPFD-GroupMe-Package_20251115_223456\start_leaderboard.ps1"""
shell.Run cmd, 0, False
