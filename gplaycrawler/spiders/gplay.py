import scrapy
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.selector import Selector
from gplaycrawler.items import GplaycrawlerItem, DynamicItem
import urllib.parse as urlparse
from collections import OrderedDict
import datetime


class MySpider(CrawlSpider):
  name = "gplay"
  allowed_domains = ["play.google.com"]
  start_urls = ["https://play.google.com/store/apps/"]
  rules = (Rule(LinkExtractor(allow=('/store/apps/details')),follow=True,callback='parse_link'), Rule(LinkExtractor(allow=('/store/apps',)),follow=True))

  def abs_url(url, response):
      """Return absolute link"""
      base = response.xpath('//head/base/@href').extract()
      if base:
        base = base[0]
      else:
        base = response.url
      return urlparse.urljoin(base, url)

  def parse_link(self,response):
      page = Selector(response)
      item = OrderedDict()
      item["Request_time"] = datetime.datetime.now()
      item["Request_url"] = response.request.url
      #item["site_content"] = response.body

      item["Link"] = page.xpath('head/link[5]/@href').get()
      item["Item_name"] = page.css('h1[itemprop=name] ::text').get()#updated
      item["Developed_by"] = page.xpath("//a[@class='hrTbp R8zArc']/text()").get()
      item["Developed_by_url"] = page.xpath("//a[@class='hrTbp R8zArc']/@href").get()
      item["Genre"] = page.xpath('//*[@itemprop="genre"]/text()').get()
      item["Video_URL"] = page.xpath('//*[@class="MMZjL lgooh  "]/@data-trailer-url').get()
      item["Screenshots"] = page.xpath("//img[@itemprop='image']/@src").getall()
      item["LogoURL"] = page.xpath("//img[@itemprop='image']/@src").get()
      item["Warnings"]= page.xpath("//div[@class='bSIuKf']/text()").getall()
      item["Ähnliche apps"] = page.xpath("//div[@class='Ktdaqe  ']").xpath("//div[@class='uzcko']//div[@class='wXUyZd']/a/@href").getall()
      item["Awards"] = page.xpath("//span[@class='GcFlrd']/text()").getall()

      try:
          item["Description_count"] = len(page.xpath("//meta[@itemprop='description']/@content").get())
          #item["Description"] = page.xpath("//meta[@itemprop='description']/@content").get()
      except:
          item["Description_count"] = 0

      try:
          item["Price"] = float(page.xpath("//meta[@itemprop='price']/@content").get().split()[0].replace(",","."))
      except:
          item["Price"] = None

      try:
          item["Rating_value"] = float(page.css('div[class=BHMmbe] ::text').get().replace(",","."))
          item["Review_number"] = int(page.css('span[class="AYi5wd TBRnV"] ::text').get().replace(",",""))#updated
      except AttributeError:
          item["Rating_value"] = None
          item["Review_number"] = 0

      Additional_Informations = page.css('div[class=hAyfc]')
      for ad in Additional_Informations:
          title_ad = ad.css('div[class=BgcNfc] ::text').get().replace("-","_")
          text_ad = ad.css('div[class=IQ1z0d] ::text').getall()
          GplaycrawlerItem.title_ad = scrapy.Field()
          item[title_ad] = text_ad

      return DynamicItem( **item )
