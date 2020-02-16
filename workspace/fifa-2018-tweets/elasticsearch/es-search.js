
exports.handleRequest = async function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});

    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    const {Client} = require('@elastic/elasticsearch')
    const esClient = new Client({node: 'http://elasticsearch:9200'})

    console.info('path : ' + parsedQuery.pathname)

    const pageNumber = parsedQuery.query.page ? parseInt(parsedQuery.query.page) : 0

    const filters = {
        pageSize: 30,
        pageNumber: pageNumber,
        tag: parsedQuery.query.tag,
        fulltext: parsedQuery.query.fulltext
    }
    if(parsedQuery.query.search_after) {
        filters.search_after = JSON.parse(parsedQuery.query.search_after)
    }
    if(parsedQuery.query.tagFacets) {
        if(Array.isArray(parsedQuery.query.tagFacets)) {
            filters.tagFacets = parsedQuery.query.tagFacets
        } else {
            filters.tagFacets = [parsedQuery.query.tagFacets]
        }
    }

    pageStart(res, "FIFA Tweets : Elasticsearch", `Tweets${filters.tag ? ' - filtrés par hashtag : ' + filters.tag : ''}${filters.fulltext ? ' - avec critère fulltext : ' + filters.fulltext : ''}`);


    await tags(esClient, res, parsedQuery);
    await tweets(esClient, res, filters);

    pageEnd(res)
    res.end()
}

async function tweets(esClient, res, filters) {
    try {

// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
        const q = {bool: {must: []}}

        if(filters.tag) {
            q.bool.must.push({match: {hashtags: filters.tag}})
        }
        if(filters.tagFacets) {
            filters.tagFacets.forEach(facet => q.bool.must.push({match: {hashtags: facet}}))

        }
        if(filters.fulltext) {
            q.bool.must.push({match: {tweet: filters.fulltext}})
        }
        if(q.bool.must.length == 0) {
            q.bool.must.push({match_all: {}})
        }

        console.log("query : ", q)
        const countResponse = await esClient.count({
            index: 'tweets',
            body: {
                query: q
            }
        })
        const search = {
            index: 'tweets',
            sort: ['date', '_id'],
            size: filters.pageSize,
            body: {
                query: q,
                aggs: {
                    tags: {terms: {field: 'hashtags'}}
                }
            }
        }

        //https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-body.html#request-body-search-search-after
        if(filters.search_after) {
            search.body.search_after = filters.search_after
        }
        const page = await esClient.search(search)

        filters.total = countResponse.body.count
        filters.start = filters.pageNumber * filters.pageSize + 1
        filters.end = Math.min(filters.start + filters.pageSize - 1, filters.total)
        if(filters.search_after) {
            filters.previous_search_after = encodeURIComponent(JSON.stringify(filters.search_after))
            filters.search_after = undefined
        }
        if(page.body.hits.hits.length > 0) {
            filters.search_after = encodeURIComponent(JSON.stringify(page.body.hits.hits[page.body.hits.hits.length - 1].sort))
        }

        console.info("FILTERS :: ", filters)
        console.info("QUERY   :: ", q)

        tweetContainerStart(res)
        tweetNavigation(res, filters)
        tagsFacets(res, page.body.aggregations.tags, filters)

        tableStart(res, "Date", "Auteur", "Tweet", "Hashtags")
        page.body.hits.hits.forEach(hit => {
            tweet = hit._source;
            tableRow(res, tweet.date, tweet.authorName, tweet.tweet, tweet.hashtags.join(', '))
        })

        tableEnd(res)
        tweetContainerEnd(res)
    } catch (e) {
        console.error("error :: " + e)
        console.error(e.stackTrace)
    }
}

async function tags(esClient, res, parsedQuery) {
    const page = await esClient.search({
        index: 'tweets',
        body: {
            aggs: {
                tags: {terms: {field: 'hashtags'}}
            }
        }
    })

    tagBarStart(res)
    tagNav(res, 'tous', null, false)

    page.body.aggregations.tags.buckets.forEach(bucket => {
        tagNav(res, bucket.key, bucket.doc_count, false)
    })
    tagBarEnd(res)
}

function tagsFacets(res, tags, filters) {
    let baseQuery = baseQueryString(filters)
    console.log("baseQuery=" + baseQuery)
    if(filters.tagFacets) {
        res.write('<p>Facettes sélectionnées : ')
        res.write(filters.tagFacets.join(', '))
        res.write('</p>')
    }

    res.write('<p>')
    tags.buckets.forEach(bucket => {
        const tag = bucket.key
        const count = bucket.doc_count
        const selected = false
        res.write(
            `
                <a class="nav-link${selected ? ' active' : ''}" href="${baseQuery}tagFacets=${tag}">${tag}</a>&nbsp;(${count}) - 
        `
        )
    })
    res.write('</p>')
}

function baseQueryString(filters) {
    let baseQuery = '?';
    if(filters.tag) {
        baseQuery += 'tag=' + filters.tag + '&';
    }
    if(filters.fulltext) {
        baseQuery += 'fulltext=' + filters.fulltext + '&'
    }
    if(filters.tagFacets) {
        filters.tagFacets.forEach(facet => baseQuery += "tagFacets=" + facet + "&")
    }
    return baseQuery
}

/*
 *
 * Formatting
 *
 */

function tagBarStart(res) {
    res.write(
        `
        <nav class="col-md-2 bg-light sidebar">
            <div class="sidebar-sticky">
                <ul class="nav flex-column">
        `
    )
}

function tagNav(res, tag, count, selected) {
    res.write(
        `
              <li class="nav-item">
                <a class="nav-link${selected ? ' active' : ''}" href="${tag != 'tous' ? '?tag=' + tag : '?'}">${tag}${count ? ' (' + count + ')' : ''}</a>
              </li>
        `
    )
}

function tagBarEnd(res) {

    res.write(
        `
            </ul>
          </div>
      </nav>
        `
    )
}

function tweetContainerStart(res) {
    res.write(
        `
        <main role="main" class="col-md-10">
        `
    )
}

function tweetContainerEnd(res) {
    res.write(
        `
        </main>
        `
    )
}

async function tweetNavigation(res, filters) {
    let baseReq = baseQueryString(filters);
    res.write(
        `
        <div>
        <nav class="navbar navbar-expand-lg">
            <ul class="nav navbar-nav pagination navbar-expand-lg navbar-light bg-light">
        `)


    if(filters.pageNumber > 0) {
        res.write(
        `
                <li class="page-item"><a class="page-link" href="${baseReq}"><span aria-hidden="true">&lt;&lt;</span></a></li>
                <li class="page-item"><a class="page-link" href="${baseReq}page=${filters.pageNumber - 1}&search_after=${filters.previous_search_after}">&lt;</a></li>
        `)
    }
    res.write(
        `
                <li class="page-item"><a class="page-link" href="#">Page ${filters.pageNumber + 1} : ${filters.start} - ${filters.end} / ${filters.total}</a></li>
        `)
    if(filters.search_after && filters.end < filters.total) {
        res.write(
        `
                <li class="page-item"><a class="page-link" href="${baseReq}page=${filters.pageNumber + 1}&search_after=${filters.search_after}"><span aria-hidden="true">&gt;</span></a></li>
        `)
    }

    res.write(
        `
            </ul>
            <ul class="nav navbar-nav pagination">
                <li>&nbsp;&nbsp;&nbsp;&nbsp;</li>
                <li>
                    <form class="form-inline" action="">
                      <input class="form-control" type="search" placeholder="Search" aria-label="Search" name="fulltext" value="${filters.fulltext ? filters.fulltext : ''}">
                      <button class="btn btn-outline-success" type="submit">Search</button>
                      <input type="hidden" name="page" value="0"/>
        `)
    if(filters.tag) {
        res.write(
            `
                      <input type="hidden" name="tag" value="${filters.tag}"/>
        `)
    }
    if(filters.tagFacets) {
        filters.tagFacets.forEach(facet => {
            res.write(
                `
                      <input type="hidden" name="tagFacets" value="${facet}"/>
                `)
        })
    }
    res.write(
        `
                    </form>
                </li>
            </ul>
        </nav>
        </div>
        `)
}

function tableStart(res, ...headers) {
    res.write(
        `
        
           <div class="tabe-responsive">
                <table class="table table-striped table-sm">
                <thead>
                    <th>${headers.join("</th><th>")}</th>
                </thead>
            
        `)
}

function tableRow(res, ...content) {
    res.write(
        `
                <tr>
                    <td>${content.join("</td><td>")}</td>
                </tr>
        
        `)
}

function tableEnd(res) {
    res.write(
        `        </table>
            </div>
                `
    )
}

function pageStart(res, title, resume) {

    res.write(
        `
        <?xml version="1.0" encoding="UTF-8" ?>
        <!DOCTYPE html>
        `
    )
    res.write(
        `
        <html xmlns="http://www.w3.org/1999/xhtml" xml:lang="fr" lang="fr" dir="ltr">
        <head>
            <title>${title}</title>
            <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css" integrity="sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu" crossorigin="anonymous">
            <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
        </head>
        <body>
            <div class="container">
                <div class="row"><h1 class="display-4 text-center">${resume}</h1></div>
                <div class="row">
        `)
}

function pageEnd(res) {
    res.write(
        `        
                </div>
            </div>
        </body>
        </html>
        `
        )
}
