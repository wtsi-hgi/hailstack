packer {
  required_plugins {
    openstack = {
      source  = "github.com/hashicorp/openstack"
      version = ">= 1.1.2"
    }
  }
}

variable "bundle_id" {
  type = string
}

variable "hail_version" {
  type = string
}

variable "spark_version" {
  type = string
}

variable "hadoop_version" {
  type = string
}

variable "java_version" {
  type = string
}

variable "python_version" {
  type = string
}

variable "scala_version" {
  type = string
}

variable "gnomad_version" {
  type = string
}

variable "base_image" {
  type = string
}

variable "ssh_username" {
  type    = string
  default = "ubuntu"
}

variable "flavor" {
  type = string
}

variable "network" {
  type = string
}

variable "floating_ip_pool" {
  type    = string
  default = ""
}

source "openstack" "hailstack" {
  image_name       = "hailstack-${var.bundle_id}"
  source_image     = var.base_image
  flavor           = var.flavor
  ssh_username     = var.ssh_username
  networks         = [var.network]
  floating_ip_pool = var.floating_ip_pool
}

build {
  sources = ["source.openstack.hailstack"]

  provisioner "shell" {
    scripts = [
      "scripts/base.sh",
      "scripts/ubuntu/packages.sh",
      "scripts/ubuntu/hadoop.sh",
      "scripts/ubuntu/spark.sh",
      "scripts/ubuntu/hail.sh",
      "scripts/ubuntu/jupyter.sh",
      "scripts/ubuntu/gnomad.sh",
      "scripts/ubuntu/uv.sh",
      "scripts/ubuntu/netdata.sh",
    ]
    environment_vars = [
      "HADOOP_VERSION=${var.hadoop_version}",
      "SPARK_VERSION=${var.spark_version}",
      "HAIL_VERSION=${var.hail_version}",
      "JAVA_VERSION=${var.java_version}",
      "PYTHON_VERSION=${var.python_version}",
      "SCALA_VERSION=${var.scala_version}",
      "GNOMAD_VERSION=${var.gnomad_version}",
    ]
  }
}