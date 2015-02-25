on run argv
    set concat to joinAList(argv, " ")
    set UnixPath to POSIX path of ((path to me as text) & "::")
    tell application "Terminal"
        tell application "System Events" to keystroke "n" using command down
        do script "cd " & UnixPath in window 1
        do script "python " & concat in window 1
    end tell
end run

on joinAList(theList, delim)
    set newString to ""
    set oldDelims to AppleScript'stext item delimiters
    set AppleScript'stext item delimiters to delim
    set newString to theList as string
    set AppleScript'stext item delimiters to oldDelims
    return newString
end joinAList
