import plotly.express as px


def plot_spectogram(df, instrument_name, start_datetime, end_datetime):
    fig = px.imshow(df.T)
    fig.update_layout(
        title=f"Spectogram of {instrument_name} from {start_datetime} to {end_datetime}",
        xaxis_title="Datetime",
        yaxis_title="Frequency",
        font=dict(family="Courier New, monospace", size=18, color="#7f7f7f"),
    )

    return fig
