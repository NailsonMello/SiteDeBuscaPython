import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re # expressão regular utilizada para fazer a separação dos caracteres
import nltk #biblioteca especifica para processamento de linguagem natural
#nltk.download() necessario fazer o download, tire o comentario eexecute somente essa linha, no pycharm vc seleciona o texto e Alt + Shift + E
import pymysql

#função que insere a localização da palavra (serve para unir a url com o id da palavra)
def inserePalavraLocalizacao(idurl, idpalavra, localizacao):
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute('insert into palavra_localizacao (idurl, idpalavra, localizacao) values (%s, %s, %s)',
                   (idurl, idpalavra, localizacao))
    idpalavra_localizacao = cursor.lastrowid
    cursor.close()
    conexao.close()
    return idpalavra_localizacao


#serve para inserir as palavras no banco de dados
def inserePalavra(palavra):
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', autocommit=True,
                              use_unicode=True, charset='utf8mb4')
    cursor = conexao.cursor()
    cursor.execute('insert into palavras (palavra) values (%s)', palavra)
    idpalavra = cursor.lastrowid
    cursor.close()
    conexao.close()
    return idpalavra

#faz a ligação entre as urls sendo que a url de origem possui o link para a url destino, onde uma aponta para outra
def insertUrlLigacao(idurl_origem, idurl_destino):
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute('insert into url_ligacao (idurl_origem, idurl_destino) values (%s, %s)',
                   (idurl_origem, idurl_destino))
    idurl_ligacao = cursor.lastrowid

    cursor.close()
    conexao.close()
    return idurl_ligacao

#faz referencia da palavra a ligação das urls
def insertUrlPalavra(idpalavra, idurl_ligacao):
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute('insert into url_palavra (idpalavra, idurl_ligacao) values (%s, %s)', (idpalavra, idurl_ligacao))
    idurl_palavra = cursor.lastrowid #pega o ultimo id que foi inserido no banco de dados

    cursor.close()
    conexao.close()
    return idurl_palavra

#verifica se ja existe ligação entre as urls
def getIdUrlLigacao(idurl_origem, idurl_destino):
    idurl_ligacao = -1
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice')
    cursor = conexao.cursor()
    cursor.execute('select idurl_ligacao from url_ligacao where idurl_origem = %s and idurl_destino = %s',
                   (idurl_origem, idurl_destino))
    if cursor.rowcount > 0: #verifica se encontrou algum registro
        idurl_ligacao = cursor.fetchone()[0]#quer dizer que encontrou algum registro
    cursor.close()
    conexao.close()
    return idurl_ligacao

#verifica se a url já esta cadastrada
def getIdUrl(url):
    idurl = -1
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', use_unicode=True,
                              charset='utf8mb4')
    cursor = conexao.cursor()
    cursor.execute('select idurl from urls where url = %s', url)
    if cursor.rowcount > 0:
        idurl = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return idurl


#verifica se a palavra já foi indexada
def palavraIndexada(palavra):
    retorno = -1  # não existe a palavra no índice
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', use_unicode=True,
                              charset='utf8mb4')
    cursor = conexao.cursor()
    cursor.execute('select idpalavra from palavras where palavra = %s', palavra)
    if cursor.rowcount > 0: #verifica se retornou algum registro
        # print("Palavra já cadastrada")
        retorno = cursor.fetchone()[0] #pega o primeiro registro
    # else:
    # print("Palavra não cadastrada")
    cursor.close()
    conexao.close()
    return retorno

#serve para inserir a url no banco de dados	
def inserePagina(url):
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice', autocommit=True,
                              use_unicode=True, charset='utf8mb4')
    cursor = conexao.cursor()
    cursor.execute('insert into urls (url) values (%s)', url)
    idpagina = cursor.lastrowid #pega o ultimo id que foi inserido no banco de dados
    cursor.close()
    conexao.close()
    return idpagina

#verifica se a pagina já foi indexada
def paginaIndexada(url):
    retorno = -1  # não existe a página
    conexao = pymysql.connect(host='localhost', user='root', passwd='123456', db='indice') #cria a conexão com o banco de dados
    cursorUrl = conexao.cursor() # cursor() é um objeto pymysql especifico para fazer consulta SQL
    cursorUrl.execute('select idurl from urls where url = %s', url) #executando a consulta no banco de dados
    if cursorUrl.rowcount > 0: #verifica se a quantidade de linhas é maior que 0
        # print("Url cadastrada")
        idurl = cursorUrl.fetchone()[0] #pega somente um dos registros
        cursorPalavra = conexao.cursor()
        cursorPalavra.execute('select idurl from palavra_localizacao where idurl = %s', idurl)#executando a consulta no banco de dados para verificar se a url já é cadastrada
        if cursorPalavra.rowcount > 0:
            # print("Url com palavras")
            retorno = -2  # existe a página com palavras cadastradas
        else:
            # print("Url sem palavras")
            retorno = idurl  # existe a página sem palavras, então retorna o ID da página

        cursorPalavra.close()
    # else:
    # print("Url não cadastrada")

    cursorUrl.close()#libera memoria
    conexao.close()#fecha a conexão

    return retorno
#essa função serve para pegar o resultado do texto, quebrar em palavras para armazenar no banco de dados, não vai pegar os stopwords, as palavras repedidas 
def separaPalavras(texto): #recebe o texto como parametro
    stop = nltk.corpus.stopwords.words('portuguese') #aqui remove os stopwords e informa que vc quer as stopwords em portugues
    stemmer = nltk.stem.RSLPStemmer() #aqui faz o stemmer (extrair o radical das palavras) diminuindo a dimensionalidade dos dados
    splitter = re.compile('\\W*') #aqui faz o splitter(expressão regular),no caso o w serve para buscar qualquer caracter que não seja uma palavra,o * diz que pode ter qualquer coisa pra frente
    lista_palavras = [] #dentro desse vetor é colocada todas as palavras que ele conseguiu encontrar
    lista = [p for p in splitter.split(texto) if p != ''] #aqui ele percorre todas as palavras que existe em um determinado texto e não pegar os caracter de espaço(vazio)
    for p in lista:#percorre a lista
        if p.lower() not in stop:#faz a verificação em letra minuscula
            if len(p) > 1: #pega somente maior que 1, para não pegar palavra somente a letra exemplo (a , b)
                lista_palavras.append(stemmer.stem(p).lower())#vai fazer a inclusão de uma nova palavra fazendo já a extração dos radicais
    return lista_palavras

def urlLigaPalavra(url_origem, url_destino):
    texto_url = url_destino.replace('_', ' ')
    palavras = separaPalavras(texto_url)
    idurl_origem = getIdUrl(url_origem)
    idurl_destino = getIdUrl(url_destino)
    if idurl_destino == -1: #se a url ainda não estiver cadastrada
        idurl_destino = inserePagina(url_destino) #cadastra a url

    if idurl_origem == idurl_destino: #se o id da url de origem for igual ao id da url destino ele sai pq não precisa dizer que uma url aponta pra ela mesma
        return

    if getIdUrlLigacao(idurl_origem, idurl_destino) > 0: #se ja existe ligação ele sai
        return

    idurl_ligacao = insertUrlLigacao(idurl_origem, idurl_destino)
    for palavra in palavras:
        idpalavra = palavraIndexada(palavra)
        if idpalavra == -1:
            idpalavra = inserePalavra(palavra)
        insertUrlPalavra(idpalavra, idurl_ligacao)

#pega todo o texto da pagina
def getTexto(sopa):
    for tags in sopa(['script', 'style']):
        tags.decompose() #serve para retirar elementos que tenham essas duas tags script e style.
    return ' '.join(sopa.stripped_strings) #retorna o texto com somente os textos


def indexador(url, sopa):
    indexada = paginaIndexada(url) #aqui verifica se a url já esta indexada
    if indexada == -2: #aqui faz a condição para dizer que a pagina já esta indexada
        print("Url já indexada")
        return
    elif indexada == -1: #caso não esteja indexada ele entra nessa condição e insere a pagina
        idnova_pagina = inserePagina(url) #insere nova pagina
    elif indexada > 0:#informa que existe a pagina mas não exite palavra
        idnova_pagina = indexada

    print("Indexando " + url)

    texto = getTexto(sopa) #aqui ele pega todo o texto da pagina que esta sendo indexada
    palavras = separaPalavras(texto) #aqui captura as palavras do texto e separa
    for i in range(len(palavras)):#faz a conta de acordo com a quantidade de palavras
        palavra = palavras[i]
        idpalavra = palavraIndexada(palavra)
        if idpalavra == -1:#informa que a palavra não existe
            idpalavra = inserePalavra(palavra)#vai inserir a palavra
        inserePalavraLocalizacao(idnova_pagina, idpalavra, i)#inserindo o id da url, o id da palavra e a localizacao, o I indica a posição da palavra

# função responsavel por fazer o crawler
def crawl(paginas, profundidade):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for i in range(profundidade):
        novas_paginas = set()
        for pagina in paginas:
            http = urllib3.PoolManager() #pega a primeira pagina
            try:
                dados_pagina = http.request('GET', pagina)#carrega os dados da pagina
            except:
                print('Erro ao abrir a página ' + pagina)
                continue

            sopa = BeautifulSoup(dados_pagina.data, "lxml")#sopa que é todo os dados da pagina
            indexador(pagina, sopa) #aqui onde comeca o indexador onde ele passa a url da pagina e a sopa que são os dados da pagina
            links = sopa.find_all('a')#pega todos os links da pagina
            contador = 1
            for link in links:#percorre todos os links da pagina, passa link por link

                if ('href' in link.attrs):
                    url = urljoin(pagina, str(link.get('href')))

                    if url.find("'") != -1:
                        continue

                    url = url.split('#')[0]

                    if url[0:4] == 'http':
                        novas_paginas.add(url)
                    urlLigaPalavra(pagina, url)

                    contador = contador + 1
            paginas = novas_paginas
        print(contador)



#teste = set()
#teste.add("a")
#teste.add("b")
#teste.add("a")

listapaginas = ['https://www.inteligenciaartificial.me/as-3-melhores-linguagens-de-programacao-para-sistemas-com-inteligencia-artificial']
crawl(listapaginas, 2)
