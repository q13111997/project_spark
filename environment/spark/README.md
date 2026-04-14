## Overview

Hướng dẫn này giúp bạn từng bước cài đặt để chạy chương trình spark trên hadoop yarn.

## 1. Setup Hadoop

Đầu tiên tiến hành cài đặt cụm hadoop.

Xem hướng dẫn cài đặt tại [đây](../../../hadoop)

## 2. Build spark docker image

Chúng ta sẽ build một custom image. Image này được cài sẵn trước 1 số chương trình như miniconda, ... dùng để submit các
job spark chạy trên yarn

```shell
docker build -t unigap/spark:3.5 .
```

## 3. Create volumes

Tiếp theo tạo các volume để lưu dữ liệu và cache lại các thư viện của spark

```shell
docker volume create spark_data
docker volume create spark_lib
```