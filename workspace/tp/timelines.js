
exports.handleRequest = async function (req, res) {
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    const cassandra = require('cassandra-driver');
    const client = new cassandra.Client({
        contactPoints: ['cassandra'],
        localDataCenter: 'datacenter1'
    });

    page(res, userTimelineData(), htagTimelineData(), selectedTweet(parsedQuery.query.selectedTweetId))
    res.end()
}

function userTimelineData() {
    return [
        ['Toto', '1231', new Date(1789, 3, 30)],
        ['Toto', '123', new Date(1795, 3, 30)],
        ['Tutu', '456', new Date(1837, 2, 4)],
        ['Titi', '7891',  new Date(1822, 2, 4)],
        ['Titi', '7892',  new Date(1818, 2, 4)],
        ['Tutu', '7893',  new Date(1801, 2, 4)]
    ]
}

function htagTimelineData() {
    return [
        ['Tag1', '101112', new Date(1797, 2, 4)],
        ['Tag2', '131415', new Date(1801, 2, 4)],
        ['Tag1', '1617181', new Date(1830, 2, 4)],
        ['Tag3', '16171812', new Date(1826, 2, 4)],
        ['Tag1', '16171813', new Date(1742, 2, 4)]
    ]
}

function selectedTweet(id) {
    if(id) {
        return {
            username: 'John Doe',
            tweetid: id,
            createdAt: new Date(),
            text: 'Donec id elit non mi porta gravida at eget metus. Fusce dapibus, tellus ac cursus commodo, tortor mauris condimentum nibh, ut fermentum massa justo sit amet risus. Etiam porta sem malesuada magna mollis euismod. Donec sed odio dui.'
        };
    } else {
        return undefined;
    }
}



function page(res, userTimelineData, htagTimelineData, selectedTweet) {
    const formattedUserTimelineData = formatTimelineData(userTimelineData);
    const formattedHtagTimelineData = formatTimelineData(htagTimelineData);

    res.write(`
    <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="fr" lang="fr" dir="ltr">
      <head>
        <title>Timelines</title>
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
            <div class="row">
                <h1>User Timeline</h1>
                <div id="user-timeline" style="height: 180px;"></div>
            </div>
            <div class="row">
                <h1>Tag Timeline</h1>
                <div id="htag-timeline" style="height: 180px;"></div>
            </div>
            <div class="row">
    `)

    if(selectedTweet) {
        res.write(`
                <h2>${selectedTweet.username} <small class="text-muted">(${selectedTweet.tweetid})</small></h2>
                <p><small class="text-muted">${selectedTweet.createdAt.toUTCString()}</small></p>
                <p>${selectedTweet.text}</p>
        `)
    }

    res.write(`     
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
            end.setFullYear(datum[2].getFullYear() + 3)
            formattedUserTimelineData.push(`['${datum[0]}', '${datum[1]}', new Date(${datum[2].getTime()}), new Date(${end.getTime()})]`)
        } catch (e) {
            console.error("failed formatting datum : ", datum, e)
        }
    })
    return formattedUserTimelineData;
}