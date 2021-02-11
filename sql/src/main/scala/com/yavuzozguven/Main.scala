package com.yavuzozguven

import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {
    val session = SparkSession.builder.master("local").appName("SQL").getOrCreate
    val df = session.read.options(Map("sep"->",", "header"-> "true")).
      csv("players_20.csv")


    df.createOrReplaceTempView("fifa")

    val df_pref_foot = session.sql("select preferred_foot,Count(preferred_foot) from fifa group by preferred_foot")
    df_pref_foot.show
    println("Preffered Foot Count")


    val df_pref_over = session.sql("select preferred_foot,AVG(overall) from fifa group by preferred_foot")
    df_pref_over.show
    println("Preffered Foot Count vs Overall Rating")


    val df_pref_wage = session.sql("select preferred_foot,AVG(wage_eur) from fifa group by preferred_foot")
    df_pref_wage.show
    println("Preffered Foot Count vs Wage")


    val df_nat = session.sql("select nationality,Count(nationality) from fifa group by nationality")
    df_nat.show
    println("Nation Wise Players Count")


    val df_nat_top20 = session.sql("select nationality,Count(nationality) from fifa group by nationality order by Count(nationality) desc limit 20")
    df_nat_top20.show(20)
    println("Nation Wise Players Counts for top 20 Country")


    val df_nat_wage_over = session.sql("select nationality,AVG(wage_eur),AVG(overall) from fifa group by nationality order by AVG(wage_eur) desc")
    df_nat_wage_over.show
    println("Nationality vs Wage vs Rating by Wage")


    val df_nat_wage_over_top10 = session.sql("select nationality,AVG(wage_eur),AVG(overall) from fifa group by nationality order by AVG(wage_eur) desc limit 10")
    df_nat_wage_over_top10.show
    println("Nationality vs Wage vs Rating top 10 Country by Wage")


    val df_nat_over_wage = session.sql("select nationality,AVG(wage_eur),AVG(overall) from fifa group by nationality order by AVG(overall) desc")
    df_nat_over_wage.show
    println("Nationality vs Wage vs Overall by Rating")


    val df_nat_over_wage_top10 = session.sql("select nationality,AVG(wage_eur),AVG(overall) from fifa group by nationality order by AVG(overall) desc limit 10")
    df_nat_over_wage_top10.show
    println("Nationality vs Wage vs Rating top 10 Country by Rating")


    val df_avg_wage_over = session.sql("select club,AVG(wage_eur),AVG(overall) from fifa group by club order by AVG(wage_eur) desc")
    df_avg_wage_over.show
    println("Club vs Wage vs Rating by Wage")


    val df_avg_wage_top10 = session.sql("select club,AVG(wage_eur),AVG(overall) from fifa group by club order by AVG(wage_eur) desc limit 10")
    df_avg_wage_top10.show
    println("Club vs Wage vs Rating top 10 Clubs by Wage")


    val df_avg_over_top10 = session.sql("select club,AVG(wage_eur),AVG(overall) from fifa group by club order by AVG(overall) desc limit 10")
    df_avg_over_top10.show
    println("Club vs Wage vs Rating top 10 Clubs by Rating")


    val df_age = session.sql("select age,Count(age) from fifa group by age order by Count(age) desc")
    df_age.show
    println("Age Count")


    val df_age_wage = session.sql("select age,AVG(wage_eur) from fifa group by age order by AVG(wage_eur) desc")
    df_age_wage.show
    println("Age vs Wage")


    val df_age_wage_over = session.sql("select age,AVG(wage_eur),AVG(overall) from fifa group by age order by AVG(overall) desc")
    df_age_wage_over.show
    println("Age vs Wage vs Overall Rating")


    val df_body_wage_over = session.sql("select body_type,AVG(wage_eur),AVG(overall) from fifa group by body_type order by AVG(wage_eur) desc")
    df_body_wage_over.show
    println("Body Type vs Wage vs Rating by Wage")


    val df_body_wage_over2 = session.sql("select body_type,AVG(wage_eur),AVG(overall) from fifa group by body_type order by AVG(overall) desc")
    df_body_wage_over2.show
    println("Body Type vs Wage vs Rating by Rating")


    val df_pen_wage = session.sql("select mentality_penalties,AVG(wage_eur) from fifa group by mentality_penalties order by AVG(wage_eur) desc")
    df_pen_wage.show
    println("Penalty vs Wage")


    val df_pen_over = session.sql("select mentality_penalties,AVG(overall) from fifa group by mentality_penalties order by AVG(overall) desc")
    df_pen_over.show
    println("Penalty vs Rating")


    val df_agg_wage = session.sql("select mentality_aggression,AVG(wage_eur) from fifa group by mentality_aggression order by AVG(wage_eur) desc")
    df_agg_wage.show
    println("Aggression vs Wage")


    val df_agg_over = session.sql("select mentality_aggression,AVG(overall) from fifa group by mentality_aggression order by AVG(overall) desc")
    df_agg_over.show
    println("Aggression vs Rating")


    val df_ht_wage = session.sql("select height_cm,AVG(wage_eur),AVG(overall) from fifa group by height_cm order by height_cm asc")
    df_ht_wage.show
    println("Height vs Wage vs Rating by Height")


    val df_ht_wage_sort = session.sql("select height_cm,AVG(wage_eur),AVG(overall) from fifa group by height_cm order by AVG(wage_eur) desc")
    df_ht_wage_sort.show
    println("Height vs Wage vs Rating by Wage")


    val df_wt_wage_sort = session.sql("select weight_kg,AVG(wage_eur),AVG(overall) from fifa group by weight_kg order by AVG(wage_eur) desc")
    df_wt_wage_sort.show
    println("Weight vs Wage vs Rating")


    val df_pot_over = session.sql("select potential,AVG(overall) from fifa group by potential order by AVG(overall) desc")
    df_pot_over.show
    println("Potential vs Rating")


    val df_tmpot_over = session.sql("select club,AVG(potential) from fifa group by club order by AVG(potential) desc")
    df_tmpot_over.show
    println("Club vs Potential")


    val df_tmpot_over_t10 = session.sql("select club,AVG(potential) from fifa group by club order by AVG(potential) desc limit 10")
    df_tmpot_over_t10.show
    println("Club vs Potential top 10")


    val df_pot_nat = session.sql("select nationality,AVG(potential) from fifa group by nationality order by AVG(potential) desc")
    df_pot_nat.show
    println("Nationality vs Potential")


    val df_pot_nat_t10 = session.sql("select nationality,AVG(potential) from fifa group by nationality order by AVG(potential) desc limit 10")
    df_pot_nat_t10.show
    println("Nationality vs Potential top 10")


    val df_age_dist = session.sql("select age,Count(age) from fifa group by age order by age asc")
    df_age_dist.show
    println("Age Count")


    val df_valeur_dist = session.sql("select value_eur,Count(value_eur) from fifa group by value_eur order by value_eur asc")
    df_valeur_dist.show
    println("Value Count")


    val df_wage_dist = session.sql("select wage_eur,Count(wage_eur) from fifa group by wage_eur order by wage_eur asc")
    df_wage_dist.show
    println("Wage Count")


    val df_ht_dist = session.sql("select height_cm,Count(height_cm) from fifa group by height_cm order by height_cm asc")
    df_ht_dist.show
    println("Height Count")


    val df_wt_dist = session.sql("select weight_kg,Count(weight_kg) from fifa group by weight_kg order by weight_kg asc")
    df_wt_dist.show
    println("Weight Count")


    val df_wf_dist = session.sql("select weak_foot,Count(weak_foot) from fifa group by weak_foot order by weak_foot asc")
    df_wf_dist.show
    println("Weak Foot Count")


    val df_rep_dist = session.sql("select international_reputation,Count(international_reputation) from fifa group by international_reputation order by international_reputation asc")
    df_rep_dist.show
    println("International Reputation Count")


    System.exit(0)
  }
}
