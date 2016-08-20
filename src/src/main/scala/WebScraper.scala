import org.jsoup.Jsoup
import org.jsoup.nodes.Document
/**
  * Scrapes data from websites
  * Created by edwardcannon on 20/08/2016.
  */

class WebScraper {
  var doc: Document = null
  /**
    * Extracts html from a webpage
    * @param url - Url of webpage
    * @return
    */
  def extractWebPage(url : String): String={
    this.doc = Jsoup.connect(url).get();
    this.doc.toString
  }
}

class WikipediaWebScraper extends WebScraper {

}
