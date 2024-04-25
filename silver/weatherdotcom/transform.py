# Databricks notebook source
df  = spark.read.json("/Volumes/prod/bronze/file_storage/weatherdotcom/25042024/")
