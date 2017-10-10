# Grafana configuration instructions

## Configuring eventmaster as a grafana datasource

1. Install the simple json datasource plugin
    ```bash
    $ grafana-cli plugins install grafana-simple-json-datasource
    ```
1. restart grafana
1. launch `eventmaster`
1. configure new Data Source (`Menu` > `Data Sources` > + `Add data source`)
    1. give it a name
    1. change `Type` to `SimpleJson`
    1. provide the url, including the `/grafana` suffix, e.g.: `http://localhost:50052/grafana`
    1. Configure direct accesss
    1. click `Save & Test`

At this point `eventmaster` is configured as a backend.


## Add Annotations to a Dashboard

To add all events to a dashboard you will need to configure an `Annotation` with `eventmaster` as the datasource:

1. click on `configure gear icon` > `Annotations` > `New`
1. Name the annotation
1. choose the appropriate datasource from earlier
1. optionally pick a color
1. click `Add`

At this point all events are rendered in all panels in the dashboard.


## Filter events

If you'd like to filter down by `datacenter` and `topic` you'll also need to add some template variables.

1. click on `configure gear icon` > `Templating` > `New`
1. provide `dc` as the name
1. select the `eventmaster` datasource
1. Set Refresh to `On Dashboard Load`
1. set `Query` to `dc`
1. click `Add`

Repeat these steps with `topic` insterad of `dc`.

In order for the variables to be provided to `eventmaster` for filtering edit the annotation:

1. click on `configure gear icon` > `Annotations` > `Edit`
1. provide the following as the `Query` field
    ```
    {"topic": "$topic", "dc": "$dc"} 
    ```
