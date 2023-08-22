FROM ubuntu:kinetic

ARG DEBIAN_FRONTEND=noninteractive

RUN apt-get -y update; \
    apt-get -y install \
        aoflagger-dev \
        automake \
        build-essential \
        clang \
        curl \
        cython3 \
        git \
        jq \
        lcov \
        libatlas3-base \
        libcfitsio-dev \
        liberfa-dev \
        libssl-dev \
        libtool \
        pkg-config \
        python3-astropy \
        python3-dev \
        python3-ipykernel \
        python3-ipython \
        python3-matplotlib \
        python3-numpy \
        python3-pandas \
        python3-scipy \
        python3-seaborn \
        python3-six \
        unzip \
        wget \
        zip \
    ; \
    rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*; \
    apt-get -y autoremove;

RUN update-alternatives --install /usr/bin/python python /usr/bin/python3 1

# copy the repository into the container in the /app directory
ADD . /app
WORKDIR /app
RUN pip install -r requirements.txt

# install the python module in the container
RUN pip install .

ENTRYPOINT /bin/bash