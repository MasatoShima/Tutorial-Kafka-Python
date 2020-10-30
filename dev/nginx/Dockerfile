# """
# Name: Dockerfile
# Description: Dockerfile
# Created by: Masato Shima
# Created on: 2020/10/30
# """

FROM nginx:latest

WORKDIR /

COPY nginx.conf /etc/nginx/nginx.conf

# Run Nginx
CMD ["nginx", "-g", "daemon off;"]

# End
