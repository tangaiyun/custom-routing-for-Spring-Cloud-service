# custom-routing-for-Spring-Cloud-service
service custom routing for Spring Cloud

deploy service with app-group-name attribute

for java add program argument : 
 --eureka.instance.app-group-name=group_1

for docker run as with -e argumment:
-e "eureka.instance.app-group-name=group_1"


