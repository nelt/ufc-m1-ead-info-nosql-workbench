
exports.handleRequest = async function (req, res) {
    res.writeHead(200, {'Content-Type': 'text/html; charset=utf-8'});

    /*
    connexion à mongo, récupération de la base de données fifa_tweets
     */
    const MongoClient = require('mongodb').MongoClient
    mogoClient = await MongoClient.connect('mongodb://mongo:27017', { useUnifiedTopology: true })
    const db = mogoClient.db("fifa_tweets")

    /*
    On parse l'url de la requête avec la librairie "url"
     */
    var url = require('url');
    const parsedQuery = url.parse(req.url, true);
    console.info('path : ' + parsedQuery.pathname)

    /*
    Puis on construit un objet filters qui contient les paramètres de la requête définissant les critère de la requête
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
    fonction de formattage du début de fichier HTML, vous pouvez passer aux fonctions intéressantes : tags et tweets
     */
    pageStart(res, "FIFA Tweets : MongoDB", `Tweets${filters.tag ? ' - filtrés par hashtag : ' + filters.tag : ''}${filters.fulltext ? ' - avec critère fulltext : ' + filters.fulltext : ''}`);

    /*
    requête et affichage des compteurs de tags
     */
    await tags(db, res, parsedQuery);

    /*
    requête et affichage des tweets correspondant au filtre courant
     */
    await tweets(db, res, filters);

    pageEnd(res)
    res.end()
}

/*
requête et affichage des compteurs de tags
 */
async function tags(db, res, parsedQuery) {
    tagBarStart(res)
    tagNav(res, 'tous', null, false)

    try {
        /*
        requête sur la collection "hashtags"
        - on crée un curseur : hashtags.find({}), aucun filtre n'est appliquée à la recherche
        - on ordonne les résultats : sort({count: -1}) signifie qu'on ordonne sur le champ count du plus grand vers le plus
        petit
        - on limite le nombre de résultats : limit(10)
         */
        const hashtags = db.collection('hashtags')
        const cursor = hashtags.find({}).sort({count: -1}).limit(10)
        while (await cursor.hasNext()) {
            const tag = await cursor.next()
            tagNav(res, tag.tag, tag.count, false)
        }
    } catch (e) {
        console.error("error :: " + e)
        console.error(e.stackTrace)
    }
    tagBarEnd(res)
}

/*
requête et affichage des tweets correspondant au filtre courant
 */
async function tweets(db, res, filters) {
    try {
        /*
        On récupère un objet représentant la collection "tweets"
         */
        const tweets = db.collection('tweets')

        /*
        On va construire dan sl'objet q le filtre de la requête à partir duquel on construira un curseur.
         */
        q = {}
        if(filters.tag) {
            /*
            Si le paramètre tag est renseigné (i.e., on a cliqué sur un des liens dans la colonne de gauche), on
            définit un filtre sur le champ hashtags.
            Pour celà, on crée dans q la propriété "hastags" et on utilise l'opérateur $eq pour spécifier qu'on veut
            les document dont le champ hastags contient la valeur filters.tag.
             */
            q['hashtags'] = {$eq: filters.tag}
        }

        // https://docs.mongodb.com/manual/text-search/
        if(filters.fulltext) {
            /*
            De la même façon, si le critère fulltext est renseigné (i.e., on a tapé du texte dans le champ de recherche)
            on utilise l'opérateur $search pour ajouter un critère de recherche plein text sur le pseudo champ $text qui
            représente l'aggrégation de tous les champs indexés en type text sur la collection.
            */
            q['$text'] = {$search: filters.fulltext}
        }

        /*
        On crée le curseur en spécifiant le filtre q ainsi que les paramètres de pagination en utilisant les méthodes
        limit et skip
         */
        const cursor = tweets
            .find(q)
            .sort({date: 1}).limit(filters.pageSize).skip(filters.pageNumber * 30)

        filters.total = await cursor.count()
        filters.lastPage = Math.floor(filters.total / filters.pageSize)
        filters.start = filters.pageNumber * filters.pageSize + 1
        filters.end = Math.min(filters.start + filters.pageSize - 1, filters.total)

        tweetContainerStart(res)
        tweetNavigation(res, filters)

        tableStart(res, "Date", "Auteur", "Tweet", "Hashtags")
        while (await cursor.hasNext()) {
            /*
            On itère sur le contenu du curseur pour afficher les tweets
             */
            const tweet = await cursor.next()
            tableRow(res, tweet.date, tweet.authorName, tweet.tweet, tweet.hashtags.join(', '))
        }
        tableEnd(res)
        tweetContainerEnd(res)
    } catch (e) {
        console.error("error :: " + e)
        console.error(e.stackTrace)
    }
}

/*
 *
 * Formattage : les fonction ci-dessous sont des fonctions d'affichage, elle ne sont pas à roprement parler intéressante
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
    var baseReq = "?"
    if(filters.tag) {
        baseReq += 'tag=' + filters.tag + '&'
    }
    if(filters.fulltext) {
        baseReq += 'fulltext=' + filters.fulltext + '&'
    }

    res.write(
        `
        <div>
        <nav class="navbar navbar-expand-lg">
            <ul class="nav navbar-nav pagination navbar-expand-lg navbar-light bg-light">
        `)
    if(filters.pageNumber > 0) {
        res.write(
        `
                <li class="page-item"><a class="page-link" href="${baseReq}page=0" aria-label="Previous"><span aria-hidden="true">&lt;&lt;</span></a></li>
                <li class="page-item"><a class="page-link" href="${baseReq}page=${filters.pageNumber - 1}">&lt;</a></li>
        `)
    }
    res.write(
        `
                <li class="page-item"><a class="page-link" href="#">Page ${filters.pageNumber + 1} : ${filters.start} - ${filters.end} / ${filters.total}</a></li>
        `)
    if(filters.pageNumber < filters.lastPage) {
        res.write(
        `
                <li class="page-item"><a class="page-link" href="${baseReq}page=${filters.pageNumber + 1}"><span aria-hidden="true">&gt;</span></a></li>
                <li class="page-item"><a class="page-link" href="${baseReq}page=${filters.lastPage}" aria-label="Next"><span aria-hidden="true">&gt;&gt;</span></a></li>
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
                      <input type="hidden" name="tag" value="${filters.tag ? filters.tag : ''}"/>
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
