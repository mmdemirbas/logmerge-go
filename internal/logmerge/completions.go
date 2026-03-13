package logmerge

import (
	"fmt"
	"io"
)

func generateBashCompletion(w io.Writer) {
	fmt.Fprint(w, `_logmerge() {
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"

    # Flags that take a file/directory argument
    case "$prev" in
        -o|--out|-l|--log|--config|--ignore-file)
            COMPREPLY=( $(compgen -f -- "$cur") )
            return 0
            ;;
        -i|--ignore|--alias|--since|--until|--min-ts-len|--ts-search-limit|--buf-read|--buf-write)
            return 0
            ;;
        --completions)
            COMPREPLY=( $(compgen -W "bash zsh fish powershell" -- "$cur") )
            return 0
            ;;
    esac

    # Complete flags
    if [[ "$cur" == -* ]]; then
        opts="
            -o --out
            -l --log
            --config
            -i --ignore
            --ignore-file
            --ignore-archives
            --alias
            -t --write-timestamp
            -b --write-block-alias
            -a --write-line-alias
            --since
            --until
            --ignore-timezone
            --min-ts-len
            --ts-search-limit
            --buf-read
            --buf-write
            --metrics
            --profile
            -p --progress
            --completions
        "
        COMPREPLY=( $(compgen -W "$opts" -- "$cur") )
        return 0
    fi

    # Default to file/directory completion for positional args (input paths)
    COMPREPLY=( $(compgen -f -- "$cur") )
    return 0
}

complete -o filenames -F _logmerge logmerge
`)
}

func generateZshCompletion(w io.Writer) {
	fmt.Fprint(w, `#compdef logmerge

_logmerge() {
    _arguments -s -S \
        '(-o --out)'{-o,--out}'[Output file path]:file:_files' \
        '(-l --log)'{-l,--log}'[Log/stats file path]:file:_files' \
        '--config[Base YAML configuration file]:file:_files -g "*.yaml *.yml"' \
        '*'{-i,--ignore}'[Gitignore-style ignore pattern]:pattern:' \
        '--ignore-file[File containing ignore patterns]:file:_files' \
        '--ignore-archives[Auto-ignore archive files]' \
        '*--alias[File alias mapping (pat=name)]:alias:' \
        '(-t --write-timestamp)'{-t,--write-timestamp}'[Prepend normalized timestamp to each line]' \
        '(-b --write-block-alias)'{-b,--write-block-alias}'[Insert separator when file source changes]' \
        '(-a --write-line-alias)'{-a,--write-line-alias}'[Prepend file alias to each line]' \
        '--since[Minimum timestamp (RFC3339)]:timestamp:' \
        '--until[Maximum timestamp (RFC3339)]:timestamp:' \
        '--ignore-timezone[Ignore timezone info in log timestamps]' \
        '--min-ts-len[Shortest timestamp length]:length:' \
        '--ts-search-limit[How far into each line to search for timestamps]:limit:' \
        '--buf-read[Read buffer size in bytes]:bytes:' \
        '--buf-write[Write buffer size in bytes]:bytes:' \
        '--metrics[Enable detailed metrics tree]' \
        '--profile[Enable CPU/memory profiling]' \
        '(-p --progress)'{-p,--progress}'[Show progress bar]' \
        '--completions[Generate shell completion script]:shell:(bash zsh fish powershell)' \
        '*:input path:_files -/'
}

_logmerge "$@"
`)
}

func generateFishCompletion(w io.Writer) {
	fmt.Fprint(w, `# Fish completions for logmerge
complete -c logmerge -s o -l out -r -F -d 'Output file path'
complete -c logmerge -s l -l log -r -F -d 'Log/stats file path'
complete -c logmerge -l config -r -F -d 'Base YAML configuration file'
complete -c logmerge -s i -l ignore -r -d 'Gitignore-style ignore pattern'
complete -c logmerge -l ignore-file -r -F -d 'File containing ignore patterns'
complete -c logmerge -l ignore-archives -d 'Auto-ignore archive files'
complete -c logmerge -l alias -r -d 'File alias mapping (pat=name)'
complete -c logmerge -s t -l write-timestamp -d 'Prepend normalized timestamp to each line'
complete -c logmerge -s b -l write-block-alias -d 'Insert separator when file source changes'
complete -c logmerge -s a -l write-line-alias -d 'Prepend file alias to each line'
complete -c logmerge -l since -r -d 'Minimum timestamp (RFC3339)'
complete -c logmerge -l until -r -d 'Maximum timestamp (RFC3339)'
complete -c logmerge -l ignore-timezone -d 'Ignore timezone info in log timestamps'
complete -c logmerge -l min-ts-len -r -d 'Shortest timestamp length'
complete -c logmerge -l ts-search-limit -r -d 'How far into each line to search for timestamps'
complete -c logmerge -l buf-read -r -d 'Read buffer size in bytes'
complete -c logmerge -l buf-write -r -d 'Write buffer size in bytes'
complete -c logmerge -l metrics -d 'Enable detailed metrics tree'
complete -c logmerge -l profile -d 'Enable CPU/memory profiling'
complete -c logmerge -s p -l progress -d 'Show progress bar'
complete -c logmerge -l completions -r -f -a 'bash zsh fish powershell' -d 'Generate shell completion script'
`)
}

func generatePowershellCompletion(w io.Writer) {
	fmt.Fprint(w, `Register-ArgumentCompleter -Native -CommandName logmerge -ScriptBlock {
    param($wordToComplete, $commandAst, $cursorPosition)

    $flags = @(
        @{ Name = '-o';                  Desc = 'Output file path (short)';                         Kind = 'file' }
        @{ Name = '--out';               Desc = 'Output file path';                                 Kind = 'file' }
        @{ Name = '-l';                  Desc = 'Log/stats file path (short)';                      Kind = 'file' }
        @{ Name = '--log';               Desc = 'Log/stats file path';                              Kind = 'file' }
        @{ Name = '--config';            Desc = 'Base YAML configuration file';                     Kind = 'file' }
        @{ Name = '-i';                  Desc = 'Gitignore-style ignore pattern (short)';           Kind = 'value' }
        @{ Name = '--ignore';            Desc = 'Gitignore-style ignore pattern';                   Kind = 'value' }
        @{ Name = '--ignore-file';       Desc = 'File containing ignore patterns';                  Kind = 'file' }
        @{ Name = '--ignore-archives';   Desc = 'Auto-ignore archive files';                        Kind = 'switch' }
        @{ Name = '--alias';             Desc = 'File alias mapping (pat=name)';                    Kind = 'value' }
        @{ Name = '-t';                  Desc = 'Prepend normalized timestamp (short)';             Kind = 'switch' }
        @{ Name = '--write-timestamp';   Desc = 'Prepend normalized timestamp to each line';        Kind = 'switch' }
        @{ Name = '-b';                  Desc = 'Insert separator on source change (short)';        Kind = 'switch' }
        @{ Name = '--write-block-alias'; Desc = 'Insert separator when file source changes';        Kind = 'switch' }
        @{ Name = '-a';                  Desc = 'Prepend file alias to each line (short)';          Kind = 'switch' }
        @{ Name = '--write-line-alias';  Desc = 'Prepend file alias to each line';                  Kind = 'switch' }
        @{ Name = '--since';             Desc = 'Minimum timestamp (RFC3339)';                      Kind = 'value' }
        @{ Name = '--until';             Desc = 'Maximum timestamp (RFC3339)';                      Kind = 'value' }
        @{ Name = '--ignore-timezone';   Desc = 'Ignore timezone info in log timestamps';           Kind = 'switch' }
        @{ Name = '--min-ts-len';        Desc = 'Shortest timestamp length';                        Kind = 'value' }
        @{ Name = '--ts-search-limit';   Desc = 'How far into each line to search for timestamps';  Kind = 'value' }
        @{ Name = '--buf-read';          Desc = 'Read buffer size in bytes';                        Kind = 'value' }
        @{ Name = '--buf-write';         Desc = 'Write buffer size in bytes';                       Kind = 'value' }
        @{ Name = '--metrics';           Desc = 'Enable detailed metrics tree';                     Kind = 'switch' }
        @{ Name = '--profile';           Desc = 'Enable CPU/memory profiling';                      Kind = 'switch' }
        @{ Name = '-p';                  Desc = 'Show progress bar (short)';                        Kind = 'switch' }
        @{ Name = '--progress';          Desc = 'Show progress bar';                                Kind = 'switch' }
        @{ Name = '--completions';       Desc = 'Generate shell completion script';                 Kind = 'completions' }
    )

    # Find the previous token to provide context-aware completions
    $tokens = $commandAst.ToString() -split '\s+'
    $prev = if ($tokens.Count -ge 2) { $tokens[-1] } else { '' }

    # If the previous token is a flag that takes a file, complete files
    $fileFlags = @('-o', '--out', '-l', '--log', '--config', '--ignore-file')
    if ($prev -in $fileFlags) {
        Get-ChildItem -Path "$wordToComplete*" -ErrorAction SilentlyContinue | ForEach-Object {
            $path = $_.Name
            if ($_.PSIsContainer) { $path += [IO.Path]::DirectorySeparatorChar }
            [System.Management.Automation.CompletionResult]::new($path, $path, 'ParameterValue', $path)
        }
        return
    }

    # If the previous token is --completions, suggest shell names
    if ($prev -eq '--completions') {
        @('bash', 'zsh', 'fish', 'powershell') | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
            [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
        }
        return
    }

    # If typing a flag, complete flag names
    if ($wordToComplete.StartsWith('-')) {
        $flags | Where-Object { $_.Name -like "$wordToComplete*" } | ForEach-Object {
            [System.Management.Automation.CompletionResult]::new($_.Name, $_.Name, 'ParameterName', $_.Desc)
        }
        return
    }

    # Default: complete file/directory paths (positional input paths)
    Get-ChildItem -Path "$wordToComplete*" -ErrorAction SilentlyContinue | ForEach-Object {
        $path = $_.Name
        if ($_.PSIsContainer) { $path += [IO.Path]::DirectorySeparatorChar }
        [System.Management.Automation.CompletionResult]::new($path, $path, 'ParameterValue', $path)
    }
}
`)
}
