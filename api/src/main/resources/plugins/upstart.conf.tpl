start on runlevel [2345]
stop on runlevel [016]
respawn
respawn limit unlimited
pre-start exec /opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
pre-stop exec /opt/${environment_namespace}/${component_application}/${component_name}/yarn-kill.py
env programDir=/opt/${environment_namespace}/${component_application}/${component_name}/
chdir /opt/${environment_namespace}/${component_application}/${component_name}/
exec sudo -u ${application_user} spark-submit --driver-java-options "-Dlog4j.configuration=file:///${programDir}log4j.properties" --class ${component_main_class} --name '${component_job_name}' --master yarn-cluster --files log4j.properties ${component_spark_submit_args} ${component_main_jar}
