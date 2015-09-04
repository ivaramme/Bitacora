Vagrant.configure("2") do |config|

   config.vm.define "kafka-server" do |config|
     config.vm.box = "ubuntu/trusty64"
     # Forwarded port
     config.vm.network :forwarded_port, guest: 2181, host: 2181
     # Forwarded port
     config.vm.network :forwarded_port, guest: 9092, host: 9092
 
     config.vm.provider :virtualbox do |vb, override|
            # Accessible only from the host using the IP defined above
            vb.name = "kafka-server"
            vb.customize ["modifyvm", :id, "--memory", 4096 ]
            vb.customize ["modifyvm", :id, "--cpus", 2 ]
     end
      
     config.vm.provision :shell, path: "provision/provision.sh"
   end
   
end
