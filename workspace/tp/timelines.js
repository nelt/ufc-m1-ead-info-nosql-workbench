
exports.handleRequest = async function (req, res) {
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    const cassandra = require('cassandra-driver');
    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });

    page(res, await userTimelineData(client), await htagTimelineData(client), await selectedTweet(parsedQuery.query.selectedTweetId, client))
    res.end()
}

async function userTimelineData(client) {
    return [
        ['ffieqa', '1012847289696600064', new Date(Date.parse('2018-06-29T23:56:26.000Z'))],
        ['?? Ótima Geração Belga  ??', '1012847293324816385', new Date(Date.parse('2018-06-29T23:56:27.000Z'))],
        ['Singularity  U Y', '1012847296860573697', new Date(Date.parse('2018-06-29T23:56:28.000Z'))],
        ['5:55', '1012847301931536384', new Date(Date.parse('2018-06-29T23:56:29.000Z'))],
        ['Özil', '1012847306041724928', new Date(Date.parse('2018-06-29T23:56:30.000Z')) ],
        [ 'PoA', '1012847312346009600', new Date(Date.parse('2018-06-29T23:56:31.000Z')) ],
        [ 'fateemahazzhr15', '1012847315185385472', new Date(Date.parse('2018-06-29T23:56:32.000Z')) ]
    ]
}

async function htagTimelineData(client) {
    return [ [ 'COLENG', '1012847291357515777', new Date(Date.parse('2018-06-29T23:56:26.000Z'))],
        [ 'WorldCup', '1012847293324816385', new Date(Date.parse('2018-06-29T23:56:27.000Z'))],
        [ 'FIFAStadiumDJ', '1012847296860573697', new Date(Date.parse('2018-06-29T23:56:28.000Z'))],
        [ 'worldcup', '1012847301931536384', new Date(Date.parse('2018-06-29T23:56:29.000Z'))],
        [ 'Win', '1012847308843749377', new Date(Date.parse('2018-06-29T23:56:30.000Z'))],
        [ 'FIFAWorldCup', '1012847312346009600', new Date(Date.parse('2018-06-29T23:56:31.000Z'))],
        [ 'FIFAStadiumDJ', '1012847315185385472', new Date(Date.parse('2018-06-29T23:56:32.000Z'))],
        [ 'FIFAStadiumDJ', '1012847320273162240', new Date(Date.parse('2018-06-29T23:56:33.000Z'))]
    ]
}

async function selectedTweet(id, client) {
    if(id) {
        return {
            username: 'Dwi Syafitri Irfan',
            tweetid: '1012847291357515777',
            createdAt: new Date(Date.parse('2018-06-29T23:56:26.000Z')),
            text: 'RT @FIFAWorldCup: So...\\r\\n\\r\\n#URUPOR ????\\r\\n#ESPRUS ????\\r\\n#FRAARG ????\\r\\n#CRODEN ????\\r\\n#BRAMEX ????\\r\\n#SWESUI ????\\r\\n#BELJPN ????\\r\\n#COLENG ?????????\\r\\n\\r\\nExci…'
        }
    } else {
        return undefined;
    }
}



function page(res, userTimelineData, htagTimelineData, selectedTweet) {
    const formattedUserTimelineData = formatTimelineData(userTimelineData);
    const formattedHtagTimelineData = formatTimelineData(htagTimelineData);

    res.write(
        `
            <?xml version="1.0" encoding="UTF-8" ?>
            <!DOCTYPE html>
            `
    )
    res.write(`
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="fr" lang="fr" dir="ltr">
      <head>
        <title>Tweet timelines</title>
        <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css" integrity="sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu" crossorigin="anonymous">
        <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
        
        <script type="text/javascript" src="https://www.gstatic.com/charts/loader.js"></script>
        <script type="text/javascript">
          google.charts.load('current', {'packages':['timeline']})
          google.charts.setOnLoadCallback(drawCharts)
          function drawCharts() {
              drawTimeline('user', [${formattedUserTimelineData.join(", ")}])
              drawTimeline('htag', [${formattedHtagTimelineData.join(", ")}])
          }
          function drawTimeline(timelineName, data) {
            var container = document.getElementById(timelineName + '-timeline')
            var chart = new google.visualization.Timeline(container)
            var dataTable = new google.visualization.DataTable()
    
            dataTable.addColumn({ type: 'string', id: 'Label' })
            dataTable.addColumn({ type: 'string', id: 'TweetId' })
            dataTable.addColumn({ type: 'date', id: 'Start' })
            dataTable.addColumn({ type: 'date', id: 'End' })
            dataTable.addRows(data)
            
            google.visualization.events.addListener(chart, 'select', function () {
                var selection = chart.getSelection();
                if (selection.length > 0) {
                  console.log(dataTable.getValue(selection[0].row, 0));
                  window.location = '?selectedTweetId=' + dataTable.getValue(selection[0].row, 1);
                }
              });
    
            chart.draw(dataTable, {tooltip: {trigger: 'none'}})
          }
        </script>
      </head>
      <body>
        <div class="container">
            
    `)

    if(selectedTweet) {
        res.write(`
            <div class="row ">
                <h1>Tweet s&eacute;lectionn&eacute; :</h1>
                <h2>${selectedTweet.username} <small class="text-muted">(${selectedTweet.tweetid})</small></h2>
                <p><small class="text-muted">${selectedTweet.createdAt.toUTCString()}</small></p>
                <p>${selectedTweet.text}</p>
                <hr/>
            </div>
        `)
    }

    res.write(`    
    
            <div class="row">
                <h1>Tag Timeline</h1>
                <div id="htag-timeline" style="height: 270px;"></div>
            </div>
            <div class="row">
                <h1>User Timeline</h1>
                <div id="user-timeline" style="height: 360px;"></div>
            </div>
        </div>
      </body>
    </html>
    `)
}

function formatTimelineData(userTimelineData) {
    const formattedUserTimelineData = []
    userTimelineData.forEach(datum => {
        try {
            const end = new Date(datum[2].getTime());
            end.setSeconds(end.getSeconds() + 2)
            formattedUserTimelineData.push(`['${datum[0]}', '${datum[1]}', new Date(${datum[2].getTime()}), new Date(${end.getTime()})]`)
        } catch (e) {
            console.error("failed formatting datum : ", datum, e)
        }
    })
    return formattedUserTimelineData
}