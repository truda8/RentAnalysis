# -*- coding: UTF-8 -*-
from numpy import select
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
import pandas as pd
import os
import json

class Spark_analyse:
    def __init__(self, data_path):
        self.data_dir = "data"
        self.data_path = data_path
        self.spark = self.init_spark()  # sparkSession对象
        self.df, self.data_num = self.read_data()
        self.areas = self.get_areas()  # 所有区

    # 初始化SparkSession对象
    def init_spark(self):
        spark = SparkSession.builder.master("local").appName("rent_analyse").getOrCreate()
        sc = spark.sparkContext
        sc.setLogLevel("ERROR")  # 设置日志级别
        return spark

    # 读取数据
    def read_data(self):
        data_df = self.spark.read.csv(self.data_path, header=True)

        # 修改数据类型
        data_df = data_df.withColumn("price", data_df["price"].cast(IntegerType()))
        data_df = data_df.withColumn("buildarea", data_df["buildarea"].cast(IntegerType()))

        int_num = data_df.count()  # 行数
        data_df.printSchema()
        return data_df, int_num

    # 获取所有区域列表
    def get_areas(self):
        area_list = self.df.select('area').distinct().collect()
        areas = [area_row.area for area_row in area_list]
        return areas

    # 统计整体房租的最小值、最大值、平均值、中位数
    def overall_rent(self):
        df = self.df
        result = df.select([F.mean("price"), F.min("price"), F.max("price")]).collect()[0]
        result_data = [
            {"data_type": "max", "value": result[2]},
            {"data_type": "avg", "value": round(result[0], 2)},
            {"data_type": "min", "value": result[1]},
        ]
        self.save_to_json("overall_rent.json", result_data)
        return result_data

    # 统计各个区的租房数量
    def rentals_num(self):
        df = self.df
        number = {"area": self.areas}
        area_group = df.groupBy("area")
        area_rows = sorted(area_group.agg({"*": "count"}).collect())
        counts = [item[1] for item in area_rows]
        number['counts'] = counts
        self.save_data("rentals_num.csv", number)
        return number

    # 统计各个区租金的最小值、最大值、平均值、中位数
    def rent_statistical(self):
        df = self.df

        area_group = df.groupBy("area")
        min_data = sorted(area_group.agg(F.min(df.price)).collect())
        max_data = sorted(area_group.agg(F.max(df.price)).collect())
        mean_data = sorted(area_group.agg(F.mean(df.price)).collect())

        # 中位数
        magic_percentile = F.expr('percentile_approx(price, 0.5)')
        median_data = area_group.agg(magic_percentile.alias('price_median')).collect()

        # 区名称、最小值、最大值、平均值、中位数
        areas = self.areas
        min_values = [min_row[1] for min_row in min_data]
        max_values = [max_row[1] for max_row in max_data]
        mean_values = [round(mean_row[1], 2) for mean_row in mean_data]
        median_values = [median_row[1] for median_row in median_data]

        statistical_data = {
            "area": areas,
            "min": min_values,
            "max": max_values,
            "mean": mean_values,
            "median": median_values
        }
        self.save_data("statistical_data.csv", statistical_data)
        return statistical_data

    # 根据分类统计
    def classification_count(self):
        df = self.df
        """按租金：
        500元以下、500-800元、800-1200元、1200-1500元、1500-2000元、2000元以上
        """
        """按面积：
        50m²以下、50-70m²、70-90m²、90-120m²、120-150m²、150m²以上
        """
        rent_count0 = df.where("price < 500").count()
        rent_count1 = df.where("price >= 500 and price < 800").count()
        rent_count2 = df.where("price >= 800 and price < 1200").count()
        rent_count3 = df.where("price >= 1200 and price < 1500").count()
        rent_count4 = df.where("price >= 1500 and price < 2000").count()
        rent_count5 = df.where("price >= 2000").count()
        rent_counts = [
            {"rent": "500元以下", "value": rent_count0},
            {"rent": "500-800元", "value": rent_count1},
            {"rent": "800-1200元", "value": rent_count2},
            {"rent": "1200-1500元", "value": rent_count3},
            {"rent": "1500-2000元", "value": rent_count4},
            {"rent": "2000元以上", "value": rent_count5}
        ]
        self.save_to_json("rent_counts.json", rent_counts)

        buildarea_count0 = df.where("buildarea < 50").count()
        buildarea_count1 = df.where("buildarea >= 50 and buildarea < 70").count()
        buildarea_count2 = df.where("buildarea >= 70 and buildarea < 90").count()
        buildarea_count3 = df.where("buildarea >= 90 and buildarea < 120").count()
        buildarea_count4 = df.where("buildarea >= 120 and buildarea < 150").count()
        buildarea_count5 = df.where("buildarea >= 150").count()
        buildarea_counts = [
            {"buildarea": "50m²以下", "value": buildarea_count0},
            {"buildarea": "50-70m²", "value": buildarea_count1},
            {"buildarea": "70-80m²", "value": buildarea_count2},
            {"buildarea": "90-120m²", "value": buildarea_count3},
            {"buildarea": "120-144m²", "value": buildarea_count4},
            {"buildarea": "150m²以上", "value": buildarea_count5}
        ]
        self.save_to_json("buildarea_counts.json", buildarea_counts)

    # 保存分析结果到csv
    def save_data(self, filename, data):
        df = pd.DataFrame(data)
        save_path = os.path.join(self.data_dir, filename)
        df.to_csv(save_path, index=False)

    # 保存为json格式
    def save_to_json(self, filename, data):
        save_path = os.path.join(self.data_dir, filename)
        with open(save_path, "w") as file:
            file.write(json.dumps(data))

if __name__ == '__main__':
    RentAnalyse = Spark_analyse("data/rent.csv")
    RentAnalyse.overall_rent()
    RentAnalyse.rentals_num()
    RentAnalyse.rent_statistical()
    RentAnalyse.classification_count()
