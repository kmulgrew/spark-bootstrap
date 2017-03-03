# spark-bootstrap

I have written all files starting with "input".  

Here are the commands required for the terminal:

assuming you have kerberos:

for input java 3

mvn package

spark-submit --driver-memory 256M --executor-memory 256M --keytab kerberos_keytab --principal kerberos_user --class mainClassName /path/to/jar path/to/folder "/[wdk]ing"




for input scala 3

mvn package

spark-submit --driver-memory 256M --executor-memory 256M --keytab kerberos_keytab --principal kerberos_user --class mainClassName /path/to/jar  path/to/folder "/[wdk]ing"
