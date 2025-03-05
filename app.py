import os 
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import Counter
import re
import plotly.express as px
import seaborn as sns

# Get the data via API from BigQuery
@st.cache_data
def get_data():
    # extract credentials from secret
    service_account_info = {
        "type": st.secrets["gcp_service_account"]["type"],
        "project_id": st.secrets["gcp_service_account"]["project_id"],
        "private_key_id": st.secrets["gcp_service_account"]["private_key_id"],
        "private_key": st.secrets["gcp_service_account"]["private_key"],
        "client_email": st.secrets["gcp_service_account"]["client_email"],
        "client_id": st.secrets["gcp_service_account"]["client_id"],
        "auth_uri": st.secrets["gcp_service_account"]["auth_uri"],
        "token_uri": st.secrets["gcp_service_account"]["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["gcp_service_account"]["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["gcp_service_account"]["client_x509_cert_url"],
    }
    # create the credentials
    credentials = Credentials.from_service_account_info(service_account_info)
    # configure the client
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
    # build the query to consult the data
    sql_query = """
    WITH 
    frases AS (
    SELECT 
        user_pseudo_id,
        MAX(IF(event_params.key IN ("term"), event_params.value.string_value, NULL)) AS utm_term,
        MAX(IF(event_params.key IN ("frase"), event_params.value.string_value, NULL)) AS frase
    FROM 
        `orbital-station-6a5ab.analytics_308291533.events_*`,
        UNNEST(event_params) AS event_params 
    WHERE PARSE_DATE('%Y%m%d', event_date) >= "2024-12-12"
    GROUP BY 
    ALL
    ),
    eventos AS (
    SELECT 
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        FORMAT_TIMESTAMP('%H:%M:%S', TIMESTAMP_MICROS(event_timestamp)) AS event_timestamp,
        event_name,
        user_pseudo_id
    FROM 
        `orbital-station-6a5ab.analytics_308291533.events_*`,
        UNNEST(event_params) AS event_params 
    WHERE PARSE_DATE('%Y%m%d', event_date) >= "2024-12-12"
        AND event_params.value.string_value LIKE "%produtos.orbital.company%"
    GROUP BY 
        ALL
    ),
    perfil AS (
    SELECT 
      user_pseudo_id,
      MAX(IF(user_properties.key IN ("buyer_behavior"), user_properties.value.string_value, NULL)) AS buyer_behavior,
      MAX(IF(user_properties.key IN ("age"), user_properties.value.string_value, NULL)) AS age,
      MAX(IF(user_properties.key IN ("marital"), user_properties.value.string_value, NULL)) AS marital,
      MAX(IF(user_properties.key IN ("connection_place"), user_properties.value.string_value, NULL)) AS connection_place,
      MAX(IF(user_properties.key IN ("everyone_category"), user_properties.value.string_value, NULL)) AS everyone_category,
      MAX(IF(user_properties.key IN ("connection_company"), user_properties.value.string_value, NULL)) AS connection_company,
      MAX(IF(user_properties.key IN ("product_interest"), user_properties.value.string_value, NULL)) AS product_interest,
      MAX(IF(user_properties.key IN ("income"), user_properties.value.string_value, NULL)) AS income,
      MAX(IF(user_properties.key IN ("connection"), user_properties.value.string_value, NULL)) AS connection,
      MAX(IF(user_properties.key IN ("gender"), user_properties.value.string_value, NULL)) AS gender,
      MAX(IF(user_properties.key IN ("education"), user_properties.value.string_value, NULL)) AS education,
      MAX(IF(user_properties.key IN ("user_interest"), user_properties.value.string_value, NULL)) AS user_interest
    FROM 
        `orbital-station-6a5ab.analytics_308291533.events_*`,
        UNNEST(user_properties) AS user_properties 
    WHERE PARSE_DATE('%Y%m%d', event_date) >= "2024-12-12"
    GROUP BY 
    ALL
    )
  
    SELECT 
        eventos.*,
        frases.utm_term,
        frases.frase,
        perfil.buyer_behavior,
        perfil.age,
        perfil.marital,
        perfil.connection_place,
        perfil.everyone_category,
        perfil.connection_company,
        perfil.product_interest,
        perfil.income,
        perfil.connection,
        perfil.gender,
        perfil.education,
        perfil.user_interest
    FROM eventos
    LEFT JOIN frases ON frases.user_pseudo_id = eventos.user_pseudo_id
    LEFT JOIN perfil ON perfil.user_pseudo_id = eventos.user_pseudo_id
    """
    # execute the consult and convert in a dataframe
    query_job = client.query(sql_query)  # execute the consult
    df_bq = query_job.to_dataframe()    # onvert into a dataframe
    # Return the results
    return df_bq

# Initialize the state of the session
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

# Function to check the password
if not st.session_state.authenticated:
    password = st.text_input("Digite a senha para acessar o dashboard:", type="password")
    if password == st.secrets["credentials"]["password"]:
        st.session_state.authenticated = True
        st.success("Senha correta! Acessando o dashboard...")
    elif password != "":
        st.error("Senha incorreta. Tente novamente.")


# Try to access the application with the given password
if st.session_state.authenticated:

    # Title of the dashboard
    st.header("Landing Page Data Analysis - Orbital")

    # get the data
    data = get_data()

    # Delete the registers with 'user_pseudo_id' filled as 'unknown' registros onde `user_pseudo_id` é "unknown"
    data = data[data["user_pseudo_id"] != "unknown"]

    # Select the unique user_pseudo_id and replace by a new identifier
    unique_users = data["user_pseudo_id"].unique()
    user_id_map = {user: idx for idx, user in enumerate(unique_users, start=1000)}

    # Substituir user_pseudo_id por user_id
    data["user_id"] = data["user_pseudo_id"].map(user_id_map)

    # Standardize conversion events
    # List of the events that need to be renamed to "lead"
    lead_events = [
        "conversion_event_contact", "envio_leads_leadster", "leadster",
        "orbital_tap_botao_whats", "new_lead", "calendly", "lead", "novo_lead___leadster"
    ]
    data["event_name"] = data["event_name"].str.lower().replace(lead_events, "lead")

    # Filters
    st.sidebar.header("Filtros")

    # Filter by "Period"
    st.sidebar.subheader("Filtrar por período")
    initial_date = data["event_date"].min() 
    final_date =  data["event_date"].max()
    date_interval = st.sidebar.slider("Selecione o período:", 
                                      min_value=initial_date, 
                                      max_value= final_date, 
                                      value=(initial_date,final_date))
    
    # Filter by "event_name"
    st.sidebar.subheader("Filtrar por Event Name")
    st.sidebar.write("Por padrão, o evento selecionado é: 'lead'")
    default_events = ['lead']
    # for event in default_events:
    #     st.sidebar.markdown(f"- {event}")
    event_names = data["event_name"].unique()
    selected_events = st.sidebar.multiselect("Selecione um ou mais eventos para análise:", ["Todos","Default"] + list(event_names), default="Default")
    #print(selected_events)

    # Filter by "utm_term"
    st.sidebar.subheader("Filtrar por UTM Term")
    utm_terms = data["utm_term"].unique()
    selected_terms = st.sidebar.multiselect("Selecione um ou mais UTM Terms para análise:", ["Todos"] + list(utm_terms), default="Todos")
    #print(selected_terms)

    # Apply the filters by event_name and UTM term
    if "Todos" in selected_events and "Todos" in selected_terms:
        filtered_data = data
    elif "Default" in selected_events and "Todos" in selected_terms: 
        filtered_data = data[data["event_name"].isin(default_events)]
        #print(filtered_data[["event_name"]].drop_duplicates())
    elif "Todos" in selected_events and "Todos" not in selected_terms:
        filtered_data = data[data["utm_term"].isin(selected_terms)]
    elif "Todos" not in selected_events and "Default" not in selected_events and "Todos" in selected_terms:
        filtered_data = data[data["event_name"].isin(selected_events)]
        #print(filtered_data[["event_name"]].drop_duplicates())
    else:
        filtered_data = data[(data["event_name"].isin(selected_events)) & (data["utm_term"].isin(selected_terms))]
    
    # Apply the period filter
    filtered_data = filtered_data[(filtered_data["event_date"] >= date_interval[0]) & (filtered_data["event_date"] <= date_interval[1])] 
    print(filtered_data["event_date"].min())
    print(filtered_data["event_date"].max())
    filtered_data.head()

    # Create the cards with the number of clients in each step
    st.subheader("Indicadores de Etapas do Funil")

    etapas = {
        "etapa - 0 - iniciar": "Iniciar Forms",
        "etapa - 1 - name": "Preencher Nome",
        "etapa - 2 - email": "Preencher E-mail",
        "etapa - 3 - phone": "Preencher Tel.",
        "lead": "Envio lead",
    }

    # Create the numbers of each step
    indicators_data = data[(data["event_date"] >= date_interval[0]) & (data["event_date"] <= date_interval[1])] 
    valores = [
        indicators_data[indicators_data["event_name"] == evento]["user_pseudo_id"].nunique()
        for evento in etapas.keys()
    ]

    # Show the cards side by side
    colunas = st.columns(len(etapas))  # Create a column for each card
    for i, coluna in enumerate(colunas):
        with coluna:
            st.metric(label=etapas[list(etapas.keys())[i]], value=valores[i])

    #st.write(f"### Dados para o evento: {selected_events}")
    #st.dataframe(filtered_data[['event_date','user_id','event_name','utm_term','frase']].sort_values("event_date"), height=300)  # Enable scroll for the table

    # Create the wordcloud
    st.subheader("Nuvem de Palavras")

    st.write("Palavras com ao menos 4 dígitos, e que aparecem na frase do evento selecionado.")

    # Select unique values
    filtered_data_terms = filtered_data[['user_id','frase','utm_term']].drop_duplicates() 
    # Workcloud of the filtered data
    all_words = " ".join(filtered_data_terms["frase"].astype(str))
    all_words_term = " ".join(filtered_data_terms["utm_term"].astype(str))

    # Remove words with less then 4 digits
    words_filtered = re.findall(r'\b\w{4,}\b', all_words)
    words_filtered_term = re.findall(r'\b\w{2,}\b', all_words_term)

    #################################################
    ############# Term Influence Info ###############
    #################################################

    # Create the wordcloud
    wordcloud_all = WordCloud(width=800, height=400, background_color="white").generate(" ".join(words_filtered))

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.imshow(wordcloud_all, interpolation="bilinear")
    ax.axis("off")
    st.pyplot(fig)

    # Show the 20 words that appear more frequently
    st.subheader("20 Palavras Mais Frequentes")

    # Count the frequency of each word
    word_counts = Counter(words_filtered).most_common(20)
    word_counts_term = Counter(words_filtered_term).most_common(20)
    # Create a dataframe to show the data
    word_freq_df = pd.DataFrame(word_counts, columns=["Palavra", "Frequência"])
    word_freq_term_df = pd.DataFrame(word_counts_term, columns=["Palavra", "Frequência"])

    # Create columns side by side
    col1, col2 = st.columns(2)

    # Exibe the table with most frequent words
    with col1:
        st.write("Palavras mais frequentes no texto")
        st.dataframe(word_freq_df, height=300)  # Enable scroll for the table)

    with col2:
        st.write("Palavras mais frequentes no utm term")
        st.dataframe(word_freq_term_df, height=300)  # Enable scroll for the table)
    # st.write("### Palavras mais frequentes no texto")
    # st.dataframe(word_freq_df)

    # Show the performance of the landing page
    st.subheader("Desempenho da LP")

    # Count unique customers by event and date
    aggregated_data = (
        filtered_data.groupby(["event_date", "event_name"])["user_pseudo_id"]
        .nunique()
        .reset_index()
        .rename(columns={"user_pseudo_id": "unique_users"})
    )

    # Line chart
    if not aggregated_data.empty:
        fig = px.line(
            aggregated_data,
            x="event_date",
            y="unique_users",
            color="event_name",
            title="Clientes únicos por evento ao longo do tempo",
            labels={"unique_users": "Clientes Únicos", "event_date": "Data"}
    )
    st.plotly_chart(fig)

    # Show the UTM term performance in generating the phrase
    st.subheader("UTM Term versus Frase")
    st.dataframe(filtered_data_terms[["user_id","frase","utm_term"]].drop_duplicates())
    #st.dataframe(filtered_data[["event_date","event_name","utm_term"]].drop_duplicates())

    ############################################
    ############## Profile Info ################
    ############################################

    # Create the features
    features = ["buyer_behavior","age","marital","connection_place","everyone_category","connection_company",
                "product_interest","income","connection","gender","education","user_interest"]
    
    # Perfis dos usuários
    st.subheader("Dados gerais sobre os perfis dos visitantes")
    cols = ["event_date","event_name","user_id"] + features

    # WordClouds Profile 
    # Create columns side by side
    col1, col2 = st.columns(2)

    # Exibe the table with most frequent words
    with col1:
        st.write("###### Comportamento de comprador")
        all_words_bb = " ".join(filtered_data[cols].drop_duplicates()["buyer_behavior"].astype(str)).replace(",","")
        all_words_bb_filtered = re.findall(r'\b\w{3,}\b', all_words_bb)
        wordcloud_bb = WordCloud(width=800, height=400, background_color="white").generate((' '.join(all_words_bb_filtered).replace("None","")))
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud_bb, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

    with col2:
        st.write("###### Produtos de interesse")
        all_words_pi = " ".join(filtered_data[cols].drop_duplicates()["product_interest"].astype(str)).replace(",","")
        all_words_pi_filtered = re.findall(r'\b\w{3,}\b', all_words_pi)
        wordcloud_pi = WordCloud(width=800, height=400, background_color="white").generate((' '.join(all_words_pi_filtered).replace("None","")))
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud_pi, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

    # Exibe the table with most frequent words
    with col1:
        st.write("###### Interesses do usuário")
        all_words_us = " ".join(filtered_data[cols].drop_duplicates()["user_interest"].astype(str)).replace(",","")
        all_words_us_filtered = re.findall(r'\b\w{3,}\b', all_words_us)
        wordcloud_us = WordCloud(width=800, height=400, background_color="white").generate((' '.join(all_words_us_filtered).replace("None","")))
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud_us, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

    with col2:
        st.write("###### Métricas gerais de perfil")
        filtered_data["income"] = filtered_data["income"].str.replace(" ","").str.replace("/","")
        filtered_data["age"] = filtered_data["age"].str.replace("-","a") 
        filtered_data['agg_profile'] = filtered_data[["age","marital","connection_place","everyone_category","connection_company","income","connection","gender","education"]].astype(str).agg(' '.join, axis=1)
        all_words_agg = " ".join(filtered_data.drop_duplicates()["agg_profile"].astype(str)).replace(",","")
        print(all_words_agg)
        all_words_agg_filtered = re.findall(r'\b[\wÀ-ÿ0-9]{3,}\b', all_words_agg)
        print(f"Filtrados: {all_words_agg_filtered}")
        wordcloud_agg = WordCloud(width=800, height=400, background_color="white").generate((' '.join(all_words_agg_filtered).replace("None","")))
        fig, ax = plt.subplots(figsize=(10, 5))
        ax.imshow(wordcloud_agg, interpolation="bilinear")
        ax.axis("off")
        st.pyplot(fig)

    # Dataframe com os dados
    st.write("##### Tabela com informações do perfil")
    st.dataframe(filtered_data[cols].drop_duplicates().sort_values("event_date", ascending=False))  


    # Conversion Rate by selected feature
    # # Include a filter to profile feature - to conversion rate
    st.subheader(f"Métricas por Perfil")
    profile_feature_selected = st.selectbox("# Selecione a característica desejada:", features)
    # Count unique user by profile feature
    cols_selected = ["event_date","event_name","user_id"] + [profile_feature_selected]
    user_per_profile = filtered_data[cols_selected].groupby(profile_feature_selected)["user_id"].nunique()
    # Count unique users by event "lead"
    lead_users_per_profile = filtered_data[cols_selected][filtered_data[cols_selected]["event_name"] == "lead"].groupby(profile_feature_selected)["user_id"].nunique()
    # Calculate conversion rate
    conversion_rate = (lead_users_per_profile / user_per_profile * 100).round(2).fillna(0)
    # Create dataframe
    conversion_df = conversion_rate.reset_index()
    conversion_df.columns = [profile_feature_selected, "conversion_rate"]
    st.write(f"##### Taxa de conversão por: {profile_feature_selected}")
    #conversion_by_x = filtered_data[cols].drop(["event_date"], axis=1).drop_duplicates().groupby(profile_feature_selected)["event_name"].apply(lambda x: (x == "lead").mean()).reset_index()
    #conversion_by_x.columns = [profile_feature_selected, "conversion_rate"]
    fig = px.bar(conversion_df, x=profile_feature_selected, y="conversion_rate",  
                 color_discrete_sequence=["#2B758E"],
                labels={  # Define os nomes personalizados dos eixos
                    profile_feature_selected: f"Perfil do Cliente: {profile_feature_selected}",  # Nome do eixo X
                    "conversion_rate": "Taxa de Conversão (%)"  # Nome do eixo Y
                })
    st.plotly_chart(fig)

    fig.update_layout(
        xaxis_title="Perfil do Cliente",
        yaxis_title="Taxa de Conversão (%)",
        yaxis=dict(tickformat=".1%")
    )

    # Distribution by selected feature
    st.write(f"##### Distribuição por: {profile_feature_selected}")
    fig = px.histogram(
    filtered_data[cols_selected].drop(["event_date","event_name"], axis=1).drop_duplicates(),  # Remove duplicatas
    x=profile_feature_selected,
    nbins=20,  # Número de bins
    #title=f"Distribuição dos Usuários",
    color_discrete_sequence=["#2B758E"],  # Define a cor principal
    )
    # Adicionando a linha KDE (densidade) manualmente
    fig.update_layout(
        xaxis_title=f"Perfil do Cliente: {profile_feature_selected}",
        yaxis_title="Frequência (Núm. Clientes)",
        bargap=0.1,  # Ajusta o espaçamento entre as barras
        showlegend=False,  # Oculta legenda para um visual mais limpo
    )
    st.plotly_chart(fig)

else:
    st.write("Aguardando inserção da senha correta para acessar o conteúdo...")