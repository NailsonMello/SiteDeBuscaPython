from bs4 import BeautifulSoup
import requests

#rrayTitulo = ""

def extrair_titulo(content):
    soup = BeautifulSoup(content, "lxml")
    tag = soup.find("title", text=True)

    if not tag:
        return None

    return tag.string.strip()


def extrair_links(content):
    soup = BeautifulSoup(content, "lxml")
    links = set()

    for tag in soup.find_all("a", href=True):
        if tag["href"].startswith("http"):
            links.add(tag["href"])

    return links


# page = requests.get("https://www.python.org/")

# links = extrair_links(page.text)

# for link in links:
#   print(link)


def crawl(url_inicial):
    url_visitada = set([url_inicial])
    url_naovisitada = set([url_inicial])

    while url_naovisitada:
        url = url_naovisitada.pop()
        try:
            content = requests.get(url, timeout=10).text
        except Exception:
            continue

        titulo = extrair_titulo(content)
        #  arrayTitulo = titulo
        if titulo:
            print(titulo)
            print(url)
            print()

        for link in extrair_links(content):
            if link not in url_visitada:
                url_visitada.add(link)
                url_naovisitada.add(link)


try:
    crawl("https://pt.wikipedia.org/wiki/Linguagem_de_programa%C3%A7%C3%A3o")
except KeyboardInterrupt:
    print()
    print("Bye!")

# TENTATIVA DE COMPARAÇÃO TO TITULO DAS URLS COM  O TITULO QUE ESTA SENDO PASSADO
#    def inserindo_titulo(tituloWeb):
    #        for titulo in arrayTitulo:
    #       if tituloWeb not in titulo:
    #           print(titulo)
#           print("Achamos")
