on run argv
  set viewPath to item 1 of argv
  set runDirectory to item 2 of argv
  set clusterContext to item 3 of argv
  set viewerCommand to item 4 of argv
  set launchPath to system attribute "PATH"
  set launchCommand to "/usr/bin/env BASH_ENV=/dev/null /bin/bash --noprofile --norc " & quoted form of viewPath & " " & quoted form of runDirectory & " " & quoted form of clusterContext & " " & quoted form of viewerCommand
  with timeout of 15 seconds
    tell application "Ghostty"
      set labConfig to new surface configuration
      set command of labConfig to launchCommand
      set initial working directory of labConfig to runDirectory
      set environment variables of labConfig to {"KUBECONFIG=" & runDirectory & "/kubeconfig", "BASH_ENV=/dev/null", "PATH=" & launchPath}
      set wait after command of labConfig to false
      set labWindow to new window with configuration labConfig
      activate window labWindow
      return id of focused terminal of selected tab of labWindow
    end tell
  end timeout
end run
