# -*- mode: ruby -*-
# vi: set ft=ruby :

$developer_name = "developer"
$developer_home = "/home/$developer_name"

group { 'dev-group' :
    ensure => present,
    name => "$developer_name",
    gid => 1234,
}

user { 'dev-user' :
    ensure => present,
    name => "$developer_name",
    uid => 1234,
    gid => 1234,
    managehome => true,
    shell => '/bin/bash',
    require => Group['dev-group'],
}

file { 'ssh-folder':
    ensure => directory,
    path => "${developer_home}/.ssh",
    group => "$developer_name",
    owner => "$developer_name",
    mode => 0600,
    require => User["$developer_name"],
}

file { 'ssh-private-key' :
    ensure => file,
    path => "${developer_home}/.ssh/id_rsa",
    source => '/vagrant_data/vm/dev/files/developer_id_rsa',
    group => 'developer',
    owner => 'developer',
    mode => 0600,
    require => File['ssh-folder'],
}

file { 'ssh-public-key' :
    ensure => file,
    path => "${developer_home}/.ssh/authorized_keys",
    source => '/vagrant_data/vm/dev/files/developer_id_rsa.pub',
    group => "$developer_name",
    owner => "$developer_name",
    mode => 0600,
    require => File['ssh-folder'],
}


$packages = [
  'build-essential',
  'libgpgme11-dev',
  'stress', # Required for automated GPG key generation
]

package { $packages :
    ensure => present,
}

exec { 'install-rust' :
    command => '/vagrant_data/env/scripts/install-rust-debian.sh',
    provider => shell,
    onlyif => 'test ! -e /usr/local/bin/rustc',
    timeout => 600,
}

exec { 'install-libressl' :
    command => '/vagrant_data/env/scripts/install-libressl-debian.sh',
    provider => shell,
    onlyif => '/usr/bin/test ! -f /usr/lib/libtls.la',
    require => Package['build-essential'],
    timeout => 600,
}

exec { 'prepare-home' :
    command => "

# Make sure home has all files from skeleton
for path in /etc/skel/.*; do
    path_in_home=\"$developer_home/\$(basename \$path)\"
    if [ -f \$path ] && [ ! -f \$path_in_home ]; then
        cp \$path  \$path_in_home
        chown $developer_name:$developer_name \$path_in_home
    fi;
    chown -R $developer_name:$developer_name $developer_home
done

tag=ZYN-DEV-ENV
sed -i \"/\$tag/,/\$tag/d\" $developer_home/.bashrc
cat <<EOF >> $developer_home/.bashrc
# \$tag
source \"\\\$HOME/.zyn-dev-env.sh\"
# /\$tag
EOF
",
    provider => shell,
    require => User["$developer_name"],
}

exec { 'prepare-gpg-test-user' :
    command => "/vagrant_data/env/scripts/prepare-encryption-keys.sh $developer_home",
    provider => shell,
    user => "$developer_name",
    timeout => 600,
    require => [User["$developer_name"], Package['stress']],
}

file { 'dev-env' :
    ensure => file,
    path => "/home/$developer_name/.zyn-dev-env.sh",
    group => "$developer_name",
    owner => "$developer_name",
    mode => 777,
    content => "

echo \"
\tZYN - Development environment

Zyn project is mounted to \\\$HOME/zyn

To have sudo access to VM, switch to \\\"vagrant\\\" user
su vagrant
password vagrant
\"

function zyn-build() {
    project_path=\$HOME/zyn/zyn
    pushd \$project_path &> /dev/null
    r=0
    cargo build || r=1
    popd &> /dev/null
    return \"\$r\"
}

"
}
