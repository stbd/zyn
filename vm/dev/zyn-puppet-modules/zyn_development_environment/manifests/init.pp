
class zyn_development_environment(
        $developer_name,
        $developer_user_pid = 1234,
        $developer_group_pid = 1234,
      ) {

        $developer_home = "/home/$developer_name"

        group { 'dev-group' :
        ensure => present,
               name => "$developer_name",
               gid => "$developer_group_pid",
        }

        user { 'dev-user' :
        ensure => present,
               name => "$developer_name",
               uid => "$developer_user_pid",
               gid => "$developer_group_pid",
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
               source => 'puppet:///modules/zyn_development_environment/developer_id_rsa',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0600,
               require => File['ssh-folder'],
        }

        file { 'ssh-public-key' :
        ensure => file,
               path => "${developer_home}/.ssh/authorized_keys",
               source => 'puppet:///modules/zyn_development_environment/developer_id_rsa.pub',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0600,
               require => File['ssh-folder'],
        }

        file { 'prepare-home-script' :
        ensure => file,
               path => "${developer_home}/.zyn-prepare-home.sh",
               source => 'puppet:///modules/zyn_development_environment/zyn-prepare-home.sh',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0776,
               require => User["$developer_name"],
        }

        exec { 'prepare-home' :
                 command => "${developer_home}/.zyn-prepare-home.sh $developer_name $developer_home",
               provider => shell,
               require => [
                 User["$developer_name"],
                 File["prepare-home-script"],
               ]
        }

        file { 'zyn-dev-env-script' :
        ensure => file,
               path => "${developer_home}/.zyn-dev-env.sh",
               source => 'puppet:///modules/zyn_development_environment/zyn-dev-env.sh',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0776,
               require => User["$developer_name"],
        }

        $packages = [
          'build-essential',
          'libgpgme11-dev',
          'stress', # Required for automated GPG key generation
          'python3',
          'python3-pip',
          'shellcheck', # Static analyser for bash scripts
        ]

        package { $packages :
        ensure => present,
        }

        exec { 'install-test-utils' :
                 command => "pip3 install -e ${developer_home}/zyn/tests",
               provider => shell,
               require => [
                 User["$developer_name"],
                 Package['python3'],
                 Package['python3-pip'],
               ]
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

        file { 'prepare-encryption-keys' :
        ensure => file,
               path => "${developer_home}/.zyn-prepare-encryption-keys.sh",
               source => 'puppet:///modules/zyn_development_environment/zyn-prepare-encryption-keys.sh',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0776,
               require => User["$developer_name"],
        }

        exec { 'prepare-encryption-keys' :
                 command => "${developer_home}/.zyn-prepare-encryption-keys.sh $developer_home",
               provider => shell,
               user => "$developer_name",
               timeout => 600,
               require => [
                 User["$developer_name"],
                 Package['stress'],
                 Exec['prepare-home'],
               ],
        }
}
