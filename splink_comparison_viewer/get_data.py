from splink.diagnostics import comparison_vector_distribution


def get_edges_data(df_e):

    spark = df_e.sql_ctx.sparkSession

    dfe_cols = df_e.columns
    gamma_cols = [c for c in dfe_cols if c.startswith("gamma_")]
    non_gamma_cols = [c for c in dfe_cols if not c.startswith("gamma_")]

    gamma_cols_sel = ", ".join(gamma_cols)

    first_aggs = [f"first({c}) as {c}" for c in non_gamma_cols]
    first_aggs_sel = ", ".join(first_aggs)

    df_e.createOrReplaceTempView("df_e")

    sql = f"""
    select {first_aggs_sel}, concat_ws(',', {gamma_cols_sel}) as gam_concat
    from df_e
    group by {gamma_cols_sel}
    """

    df_e_to_join = spark.sql(sql).toPandas()

    cvd = comparison_vector_distribution(df_e)
    if "match_probability" in cvd:
        cvd = cvd.drop("match_probability", axis=1)
    if "match_weight" in cvd:
        cvd = cvd.drop("match_weight", axis=1)

    data_for_vis = cvd.merge(df_e_to_join, on="gam_concat", how="left")

    return data_for_vis
