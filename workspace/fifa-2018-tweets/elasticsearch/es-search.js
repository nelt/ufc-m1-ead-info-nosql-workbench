
exports.handleRequest = async function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});

    /*
    Connexion à Elasticsearch
     */
    const {Client} = require('@elastic/elasticsearch')
    const esClient = new Client({node: 'http://elasticsearch:9200'})

    /*
    On parse l'url de la requête avec la librairie "url"
     */
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);

    /*
    Puis on construit un objet filters qui contient les paramètres de la requête définissant les critères de la requête
    et de pagination
     */
    const pageNumber = parsedQuery.query.page ? parseInt(parsedQuery.query.page) : 0
    const filters = {
        pageSize: 30,
        pageNumber: pageNumber,
        tag: parsedQuery.query.tag,
        fulltext: parsedQuery.query.fulltext
    }
    /*
    Elasticsearch ne permet pas une pagination simple comme MongoDB. Dès que l'index est un peu volumineux, on doit
    utiliser le mécanisme de search_after. Ce mécanisme est décrit dans l'étude de cas.
     */
    if(parsedQuery.query.search_after) {
        filters.search_after = JSON.parse(parsedQuery.query.search_after)
    }
    if(parsedQuery.query.tagFacets) {
        /*
        La version Elasticsearch permet de filtrer de façon itérative sur les tag (on parle de navigation par facette)
         */
        if(Array.isArray(parsedQuery.query.tagFacets)) {
            filters.tagFacets = parsedQuery.query.tagFacets
        } else {
            filters.tagFacets = [parsedQuery.query.tagFacets]
        }
    }

    pageStart(res, "FIFA Tweets : Elasticsearch", `Tweets${filters.tag ? ' - filtrés par hashtag : ' + filters.tag : ''}${filters.fulltext ? ' - avec critère fulltext : ' + filters.fulltext : ''}`);

    /*
    Requête et affichage des dix tags les plus utilisés
     */
    await tags(esClient, res, parsedQuery);
    /*
    Requête et affichage des tweets + des facettes par tags
     */
    await tweets(esClient, res, filters);

    pageEnd(res)
    res.end()
}

/*
Requête et affichage des dix tags les plus utilisés
 */
async function tags(esClient, res, parsedQuery) {
    /*
    On réalise une requête sur tous les documents de l'index tweet sans leur appliquer de filtres. On ne s'intéresse en
    fait qu'à l'agrégation des termes du champs hashtags. Par défaut l'agrégation renvoie les dix premières valeurs
     */
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
        /*
        On itère sur les valeurs de l'agrégation.
        Chaque valeur est un objet bucket dont les deux champs nous intéressant sont :
        - bucket.key : la valeur du tag
        - bucket.doc_count : le nombre document de l'index contenant ce tag.

        Si on avait spécifié un filtre à la requête, les compteurs auraient contenu le nombre de document correspondant
        au filtre et contenant le tag.
         */
        tagNav(res, bucket.key, bucket.doc_count, false)
    })
    tagBarEnd(res)
}

async function tweets(esClient, res, filters) {
    try {

// https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
        /*
        On définit dans q le filtre sur les documents de l'index "tweets"
        Ici, on définit un filtre booléen avec une liste vide de critère additif (must: [])
         */
        const q = {bool: {must: []}}

        if(filters.tag) {
            /*
            Si le critère tag est défini, on ajoute un critère à l'expression booléenne : le champ hashtags doit
            contenir la valeur filters.tag
             */
            q.bool.must.push({match: {hashtags: filters.tag}})
        }
        if(filters.tagFacets) {
            /*
            Si des facettes sont définies, c'est-à-dire que l'utilisateur a sélectionner en plus le la recherche des
            tags dans la liste sous la barre de recherche, on ajoute un critère supplémentaire pour chaque tag
             */
            filters.tagFacets.forEach(facet => q.bool.must.push({match: {hashtags: facet}}))

        }
        if(filters.fulltext) {
            /*
            Si le critère fulltext est renseigné (i.e., on a tapé du texte dans le champ de recherche), on ajoute un critère
            sur le champ tweet.
             */
            q.bool.must.push({match: {tweet: filters.fulltext}})
        }
        if(q.bool.must.length == 0) {
            /*
            Enfi, il se peut qu'aucun critère n'ait été spécifié. Dans ce cas, la liste de critère must est vide ce qui
            va faire planter la requête à Elasicsearch. On ajoute donc un critère qui match tous les documents de l'index.
             */
            q.bool.must.push({match_all: {}})
        }

        console.log("query : ", q)
        /*
        On a un compteur de résultats total sur le résultat d'un filtre, mais, ce compteur est approximatif.
        Elasticsearch propose un point d'entrée spécifique pour récupérer le nombre exacte de documents correspondant à une
        requête, c'est ce qu'on utilise ici.
        En fait, le comptage est une fonctionnalité très gourmande en ressource. Elle est découragée...
         */
        const countResponse = await esClient.count({
            index: 'tweets',
            body: {
                query: q
            }
        })
        /*
        On exécute la requête sur l'indexe "tweets".
        Elle est ordonnées par :
        - date    : parce que c'est ce qu'on veut
        - par _id : pour pouvoir utiliser la fonctionnalité search_after, cf. cas d'usage

        On définit enfin l'agrégation des termes du champs "hashtags". Cela nous permettra d'avoir les les facettes
        correspondant au filtre sur ce champ.
         */
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

        /*
        S'il s'agit d'une requête pour la page suivante, on ajoute le champ search_after
         */
        if(filters.search_after) {
            search.body.search_after = filters.search_after
        }
        const page = await esClient.search(search)

        filters.total = countResponse.body.count
        filters.start = filters.pageNumber * filters.pageSize + 1
        filters.end = Math.min(filters.start + filters.pageSize - 1, filters.total)
        if(filters.search_after) {
            filters.search_after = undefined
        }
        if(page.body.hits.hits.length > 0) {
            filters.search_after = encodeURIComponent(JSON.stringify(page.body.hits.hits[page.body.hits.hits.length - 1].sort))
        }

        tweetContainerStart(res)
        tweetNavigation(res, filters)
        /*
        Affichage des facettes sur le champ hashtags
         */
        tagsFacets(res, page.body.aggregations.tags, filters)

        tableStart(res, "Date", "Auteur", "Tweet", "Hashtags")
        page.body.hits.hits.forEach(hit => {
            /*
            On itère sur les résultats de la requête et on affiche les tweets
             */
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

/*
Affichage des facettes sur le champ hashtags
 */
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
 * Formatage : les fonctions ci-dessous sont des fonctions d'affichage, elle ne sont pas à proprement parler intéressantes
 * pour le cours, mais, si vous souhaitez comprendre le fonctionnement du script... allez-y !
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
