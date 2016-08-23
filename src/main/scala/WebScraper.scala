import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.jsoup.select.Elements
import scala.collection.JavaConversions._

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

  def extractLinks(): List[Elements]={
    val links: Elements = this.doc.getElementsByTag("a");
    val ahrefs = List(links)
    ahrefs
    }
}

class WikipediaWebScraper extends WebScraper {

}
