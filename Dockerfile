FROM astrocrpublic.azurecr.io/runtime:3.1-9


USER root
RUN apt-get update && apt-get install -y curl && apt-get clean
USER astro