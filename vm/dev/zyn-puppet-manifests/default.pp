# -*- mode: ruby -*-
# vi: set ft=ruby :

exec { 'Update-apt' :
    command => 'apt-get update',
    provider => shell,
    timeout => 600,
}

class { 'zyn_development_environment' :
          developer_name => "$user",
        developer_user_pid => "$user_id",
        developer_group_pid => "$user_group_id",
        require => Exec['Update-apt']
}
