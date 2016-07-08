node {

   stage 'Build'
   def workspace = pwd() 

   sh '''
   	echo $PWD
   	echo $BRANCH_NAME
   	cd $PWD@script/api;mvn versions:set -DnewVersion=$BRANCH_NAME
	   mvn clean package
	'''

   stage 'Test'
   sh '''
	  cd $PWD@script/api/src/main/resources/; 
     pylint_wrapper.py 10
	  nosetests test_*.py
   '''

   stage 'Deploy' 
   build job: 'deploy-component', parameters: [[$class: 'StringParameterValue', name: 'branch', value: env.BRANCH_NAME],[$class: 'StringParameterValue', name: 'component', value: "deployment-manager"],[$class: 'StringParameterValue', name: 'release_path', value: "platform/releases"],[$class: 'StringParameterValue', name: 'release', value: "${workspace}@script/api/target/deployment-manager-${env.BRANCH_NAME}.tar.gz"]]


}