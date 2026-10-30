FROM public.ecr.aws/docker/library/busybox:latest
COPY app /w/cgi-bin/app
RUN echo ok > /w/health
CMD ["httpd","-f","-p","8000","-h","/w"]
