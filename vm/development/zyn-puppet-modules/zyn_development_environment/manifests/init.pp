
class zyn_development_environment(
        $developer_name,
        $developer_user_pid = 1234,
        $developer_group_pid = 1234,
      ) {

        $developer_home = "/home/$developer_name"

        $packages = [
          'build-essential',
          'haveged',            # Used to generate randomness for security operations
          'python3',
          'python3-pip',
          'shellcheck',         # Static analyser for bash scripts
          'curl',
          'gnupg2',
          'apt-transport-https',
          'software-properties-common',
	  'ssl-cert',
        ]

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
               groups => ['sudo', 'docker', 'ssl-cert'],
               managehome => true,
               shell => '/bin/bash',
               require => [
                   Group['dev-group'],
		   Package['ssl-cert'],
	           Exec['Install Docker Community Edition (CE)'],
	       ]
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
                 command => "${developer_home}/.zyn-prepare-home.sh $developer_name $developer_home $developer_home/zyn/vm/development/zyn-puppet-modules/zyn_development_environment/files",
               provider => shell,
               require => [
                 User["$developer_name"],
                 File["prepare-home-script"],
               ]
        }

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

        file { 'zyn-install-rust-debian-script' :
        ensure => file,
               path => "${developer_home}/.zyn-install-rust-debian.sh",
               source => 'puppet:///modules/zyn_development_environment/zyn-install-rust-debian.sh',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0776,
               require => [
                 Package["curl"],
                 User["$developer_name"],
               ],
        }

        exec { 'install-rust' :
                 command => "${developer_home}/.zyn-install-rust-debian.sh",
               provider => shell,
               onlyif => "/usr/bin/test ! -f $developer_home/.cargo/bin/rustup",
               user => "$developer_name",
               timeout => 600,
               require => [
                 File["zyn-install-rust-debian-script"],
                 User["$developer_name"],
                 Exec['prepare-home'],
               ],
        }

        file { 'zyn-install-libressl-debian-script' :
        ensure => file,
               path => "${developer_home}/.zyn-install-libressl-debian.sh",
               source => 'puppet:///modules/zyn_development_environment/zyn-install-libressl-debian.sh',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0776,
               require => User["$developer_name"],
        }

        exec { 'install-libressl' :
                 command => "${developer_home}/.zyn-install-libressl-debian.sh",
               provider => shell,
               onlyif => '/usr/bin/test ! -f /usr/lib/libtls.la',
               require => [
                   File['zyn-install-libressl-debian-script'],
                   Package['build-essential'],
               ],
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
                 Package['haveged'],
                 Exec['prepare-home'],
               ],
        }

        file { 'prepare-certificates-script' :
        ensure => file,
               path => "${developer_home}/.zyn-prepare-certificates.sh",
               source => 'puppet:///modules/zyn_development_environment/zyn-prepare-certificates.sh',
               group => "$developer_name",
               owner => "$developer_name",
               mode => 0776,
               require => User["$developer_name"],
        }

        exec { 'prepare-certificates' :
                 command => "${developer_home}/.zyn-prepare-certificates.sh",
               provider => shell,
               timeout => 600,
               require => [
	         Package['ssl-cert'],
                 User["$developer_name"],
               ],
        }

        exec { 'Install Docker Community Edition (CE)':
                 command => 'curl -fsSL https://download.docker.com/linux/debian/gpg | sudo apt-key add - ;
         add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" ;
         apt-get update ;
         apt-get -y install docker-ce ;',
               path => ['/usr/local/sbin', '/usr/sbin', '/sbin', '/usr/bin/', '/bin'],
               unless => 'which docker',
                      require => [
                        Package['apt-transport-https'],
                        Package['curl'],
                        Package['gnupg2'],
                        Package['software-properties-common'],
                      ]
        }
}
