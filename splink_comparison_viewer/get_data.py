from splink.diagnostics import comparison_vector_distribution
from pyspark.sql.functions import expr


def get_edges_data(df_e, num_rows_per_comparison_vector=1, salt_num_rows=20):

    salt_num_rows = max(salt_num_rows, num_rows_per_comparison_vector)

    spark = df_e.sql_ctx.sparkSession

    dfe_cols = df_e.columns
    gamma_cols = [c for c in dfe_cols if c.startswith("gamma_")]
    non_gamma_cols = [c for c in dfe_cols if not c.startswith("gamma_")]

    first_aggs = [f"first({c}) as {c}" for c in non_gamma_cols]
    first_aggs_sel = ", ".join(first_aggs)

    # The first() function is very fast because it selects the first record before the shuffle, not after
    # i.e. it does a partial sort on each executor, selects the first record, and shuffles just the first record

    # We add a salt since some of the comparison vectors are big, and we don't want to have to sort the whole thing
    # it's faster to sort within a salt, and then take

    # In addition, this means that we can select multiple first rows, one for each salt.

    # Testing showed this to be the fastest strategy on large datasets

    df_e = df_e.withColumn("__salt_performance", expr(f"floor(rand()*{salt_num_rows})"))

    gamma_cols_sel = ", ".join(gamma_cols)

    gamma_cols.append("__salt_performance")
    gamma_cols_groupby_performance = ", ".join(gamma_cols)

    df_e.createOrReplaceTempView("df_e")
    sql = f"""
    select {first_aggs_sel}, concat_ws(',', {gamma_cols_sel}) as gam_concat, __salt_performance
    from df_e
    group by {gamma_cols_groupby_performance}
    """

    df_e_to_join = spark.sql(sql)

    df_e_to_join.createOrReplaceTempView("df_e_to_join")

    sql = f"""
    with df as (
    select *, row_number() OVER (PARTITION BY gam_concat order by __salt_performance) as row_example_index
    from df_e_to_join
    )
    select *
    from df
    where  df.row_example_index <= {num_rows_per_comparison_vector}

    """

    df_e_to_join = spark.sql(sql).drop("__salt_performance").toPandas()

    cvd = comparison_vector_distribution(df_e)
    if "match_probability" in cvd:
        cvd = cvd.drop("match_probability", axis=1)
    if "match_weight" in cvd:
        cvd = cvd.drop("match_weight", axis=1)

    data_for_vis = cvd.merge(df_e_to_join, on="gam_concat", how="left")

    return data_for_vis
