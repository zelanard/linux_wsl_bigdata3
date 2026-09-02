# ~/.bashrc: executed by bash(1) for non-login shells.
# see /usr/share/doc/bash/examples/startup-files (in the package bash-doc)
# for examples

# If not running interactively, don't do anything
case $- in
    *i*) ;;
      *) return;;
esac

# don't put duplicate lines or lines starting with space in the history.
# See bash(1) for more options
HISTCONTROL=ignoreboth

# append to the history file, don't overwrite it
shopt -s histappend

# for setting history length see HISTSIZE and HISTFILESIZE in bash(1)
HISTSIZE=1000
HISTFILESIZE=2000

# check the window size after each command and, if necessary,
# update the values of LINES and COLUMNS.
shopt -s checkwinsize

# If set, the pattern "**" used in a pathname expansion context will
# match all files and zero or more directories and subdirectories.
#shopt -s globstar

# make less more friendly for non-text input files, see lesspipe(1)
[ -x /usr/bin/lesspipe ] && eval "$(SHELL=/bin/sh lesspipe)"

# set variable identifying the chroot you work in (used in the prompt below)
if [ -z "${debian_chroot:-}" ] && [ -r /etc/debian_chroot ]; then
    debian_chroot=$(cat /etc/debian_chroot)
fi

# set a fancy prompt (non-color, unless we know we "want" color)
case "$TERM" in
    xterm-color|*-256color) color_prompt=yes;;
esac

# uncomment for a colored prompt, if the terminal has the capability; turned
# off by default to not distract the user: the focus in a terminal window
# should be on the output of commands, not on the prompt
#force_color_prompt=yes

if [ -n "$force_color_prompt" ]; then
    if [ -x /usr/bin/tput ] && tput setaf 1 >&/dev/null; then
	# We have color support; assume it's compliant with Ecma-48
	# (ISO/IEC-6429). (Lack of such support is extremely rare, and such
	# a case would tend to support setf rather than setaf.)
	color_prompt=yes
    else
	color_prompt=
    fi
fi

if [ "$color_prompt" = yes ]; then
    PS1='${debian_chroot:+($debian_chroot)}\[\033[01;32m\]\u@\h\[\033[00m\]:\[\033[01;34m\]\w\[\033[00m\]\$ '
else
    PS1='${debian_chroot:+($debian_chroot)}\u@\h:\w\$ '
fi
unset color_prompt force_color_prompt

# If this is an xterm set the title to user@host:dir
case "$TERM" in
xterm*|rxvt*)
    PS1="\[\e]0;${debian_chroot:+($debian_chroot)}\u@\h: \w\a\]$PS1"
    ;;
*)
    ;;
esac

# enable color support of ls and also add handy aliases
if [ -x /usr/bin/dircolors ]; then
    test -r ~/.dircolors && eval "$(dircolors -b ~/.dircolors)" || eval "$(dircolors -b)"
    alias ls='ls --color=auto'
    #alias dir='dir --color=auto'
    #alias vdir='vdir --color=auto'

    alias grep='grep --color=auto'
    alias fgrep='fgrep --color=auto'
    alias egrep='egrep --color=auto'
fi

# colored GCC warnings and errors
#export GCC_COLORS='error=01;31:warning=01;35:note=01;36:caret=01;32:locus=01:quote=01'

# some more ls aliases
alias ll='ls -alF'
alias la='ls -A'
alias l='ls -CF'

# Add an "alert" alias for long running commands.  Use like so:
#   sleep 10; alert
alias alert='notify-send --urgency=low -i "$([ $? = 0 ] && echo terminal || echo error)" "$(history|tail -n1|sed -e '\''s/^\s*[0-9]\+\s*//;s/[;&|]\s*alert$//'\'')"'

# Alias definitions.
# You may want to put all your additions into a separate file like
# ~/.bash_aliases, instead of adding them here directly.
# See /usr/share/doc/bash-doc/examples in the bash-doc package.

if [ -f ~/.bash_aliases ]; then
    . ~/.bash_aliases
fi

# enable programmable completion features (you don't need to enable
# this, if it's already enabled in /etc/bash.bashrc and /etc/profile
# sources /etc/bash.bashrc).
if ! shopt -oq posix; then
  if [ -f /usr/share/bash-completion/bash_completion ]; then
    . /usr/share/bash-completion/bash_completion
  elif [ -f /etc/bash_completion ]; then
    . /etc/bash_completion
  fi
fi

#JONAS
# Hadoop
export HADOOP_HOME=/home/zelanard/BigData/hadoop-3.5.0
export HADOOP_INSTALL="$HADOOP_HOME"
export HADOOP_MAPRED_HOME="$HADOOP_HOME"
export HADOOP_COMMON_HOME="$HADOOP_HOME"
export HADOOP_HDFS_HOME="$HADOOP_HOME"
export YARN_HOME="$HADOOP_HOME"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export HADOOP_COMMON_LIB_NATIVE_DIR="$HADOOP_HOME/lib/native"
export HADOOP_OPTS="-Djava.library.path=$HADOOP_COMMON_LIB_NATIVE_DIR"
export PATH="$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH"

# Spark
export SPARK_HOME=/home/zelanard/BigData/spark-4.2.0-bin-hadoop3
export PATH="$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH"

#All
export PATH="$HOME/.local/bin:$PATH"

# IRIS ETL
_iris_etl() {
    local iris_project="$HOME/BigData/iris_"
    local iris_spark="$HOME/BigData/spark-4.2.0-bin-hadoop3"
    local iris_hadoop="$HOME/BigData/hadoop-3.5.0"

    (
        cd "$iris_project" || exit 1

        HADOOP_HOME="$iris_hadoop" \
        HADOOP_CONF_DIR="$iris_hadoop/etc/hadoop" \
        "$iris_spark/bin/spark-submit" \
            --master spark://localhost:7077 \
            main.py "$@"
    )
}

_iris_listen() {
    local iris_project="$HOME/BigData/iris_"
    local iris_spark="$HOME/BigData/spark-4.2.0-bin-hadoop3"
    local iris_hadoop="$HOME/BigData/hadoop-3.5.0"
    local iris_state_dir="$HOME/.local/state/burning_plumber"
    local iris_log_file="$iris_state_dir/listener.log"
    local iris_pid_file="$iris_state_dir/listener.pid"
    local iris_listener_pid

    if [[ -r "$iris_pid_file" ]]; then
        read -r iris_listener_pid < "$iris_pid_file"
        if [[ "$iris_listener_pid" =~ ^[0-9]+$ ]] \
            && kill -0 "$iris_listener_pid" 2>/dev/null; then
            echo "Iris listener is already running (PID $iris_listener_pid)"
            return 0
        fi
    fi

    command mkdir -p -- "$iris_state_dir" || return 1
    (
        nohup env \
            HADOOP_HOME="$iris_hadoop" \
            HADOOP_CONF_DIR="$iris_hadoop/etc/hadoop" \
            "$iris_spark/bin/spark-submit" \
                --master spark://localhost:7077 \
                "$iris_project/main.py" listen "$@" \
                </dev/null >"$iris_log_file" 2>&1 &
    )

    echo "Iris listener started in the background"
    echo "Log: $iris_log_file"
}

iris() {
    case "${1:-}" in
        extract|flow|stop)
            _iris_etl "$@"
            ;;
        listen)
            shift
            _iris_listen "$@"
            ;;
        print)
            local iris_log_file iris_status
            iris_log_file="$(mktemp)"
            _iris_etl print 2>"$iris_log_file"
            iris_status=$?
            if (( iris_status != 0 )); then
                command cat "$iris_log_file" >&2
            fi
            command rm -f -- "$iris_log_file"
            return "$iris_status"
            ;;
        reset)
            read -r -p "Delete all Iris pipeline data? [y/N] " answer
            [[ "$answer" =~ ^[Yy]$ ]] && _iris_etl reset
            ;;
        help|"")
            echo "Usage: iris {extract|flow|listen|print|stop|reset}"
            ;;
        *)
            echo "Unknown Iris command: $1" >&2
            return 1
            ;;
    esac
}

_iris_completion() {
    local current="${COMP_WORDS[COMP_CWORD]}"

    if (( COMP_CWORD == 1 )); then
        COMPREPLY=(
            $(compgen -W "extract flow listen print stop reset" -- "$current")
        )
    elif [[ "${COMP_WORDS[1]}" == "listen" ]]; then
        COMPREPLY=(
            $(compgen -W "--poll-interval" -- "$current")
        )
    fi
}

complete -F _iris_completion iris
