# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, String, DateTime, Text, UnicodeText
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from BiggerPockets.items import postItem, userItem

Base = declarative_base()
class Posts(Base):
    __tablename__ = 'forumposts'
    pid = Column(Integer, primary_key=True) # post id
    URL = Column(String(500))
    title = Column(String(500))
    category = Column(String(500)) # discussion category
    categoryURL = Column(String(500))
    uid = Column(Integer, ForeignKey('forumusers.uid', ondelete='CASCADE')) # user id
    replyTo = Column(Integer) # This is the first post id of the discussion
    postTime = Column(DateTime(timezone=True)) # precise to hour eg. 2017-02-11 19:00:00
    body = Column(Text)

class Users(Base):
    __tablename__ = 'forumusers'

    uid = Column(Integer, primary_key=True) # user id
    firstName = Column(String(20))
    lastName = Column(String(20))
    source = Column(String(100)) # URL of the user profile
    colleagues = Column(Integer)
    followers = Column(Integer)
    following = Column(Integer)
    numPosts = Column(Integer)
    numVotes = Column(Integer)
    numAwards = Column(Integer)
    account = Column(String(10)) # account type: base, plus, pro
    city = Column(String(100))
    state = Column(String(50))
    dateJoined = Column(DateTime) # creation date of the user account
    seeking = Column(Text) # currently seeking
    experience = Column(Text) # real estate experience
    occupation = Column(String(767))
    goals = Column(Text) # real estate goals

class BiggerpocketsPipeline(object):
    def open_spider(self, spider):
        connStr = 'mysql+mysqldb://root:931005@127.0.0.1/us'
        self.engine = create_engine(connStr, convert_unicode=True, echo=False)
        self.DB_session = sessionmaker(bind=self.engine)
        self.session = self.DB_session()
        Base.metadata.create_all(self.engine)
        self.count = 0
        
    def close_spider(self, spider):
        self.session.commit()
        self.session.close()
        self.engine.dispose()
        
    def process_item(self, item, spider):
        if isinstance(item, postItem):
            return self.handlePost(item, spider)
        if isinstance(item, userItem):
            return self.handleUser(item, spider)
                    
    def handlePost(self, item, spider):
        self.count += 1
        # commit every 100 posts
        if self.count % 100 == 0:
            self.session.commit()
            
        post = Posts(pid=item.get('pid'),
                     URL=item.get('URL'),
                     title=item.get('title'),
                     category=item.get('category'),
                     categoryURL=item.get('categoryURL'),
                     uid=item.get('uid'),
                     replyTo=item.get('replyTo'),
                     postTime=item.get('postTime'),
                     body=item.get('body'),
        )
        self.session.add(post)
        self.session.flush()
        return item
        
    def handleUser(self, item, spider):
        user =Users(uid=item.get('uid'),
                    firstName=item.get('firstName'),
                    lastName=item.get('lastName'),
                    source=item.get('source'),
                    colleagues=item.get('colleagues'),
                    followers=item.get('followers'),
                    following=item.get('following'),
                    numPosts=item.get('numPosts'),
                    numVotes=item.get('numVotes'),
                    numAwards=item.get('numAwards'),
                    account=item.get('account'),
                    city=item.get('city'),
                    state=item.get('state'),
                    dateJoined=item.get('dateJoined'),
                    seeking=item.get('seeking'),
                    occupation=item.get('occupation'),
                    experience=item.get('experience'),
                    goals=item.get('goals'),
        )
        self.session.add(user)
        self.session.flush()
        self.session.commit()
        return item
            
class DuplicatesPipeline(object):
    def __init__(self):
        connStr = 'mysql+mysqldb://root:931005@127.0.0.1/us'
        self.engine = create_engine(connStr, convert_unicode=True, echo=False)
        self.DB_session = sessionmaker(bind=self.engine)
        self.session = self.DB_session()
        self.users = set()
        self.posts = set()
        u = self.session.execute('select uid from forumusers')
        p = self.session.execute('select pid from forumposts')
        for i in u:
            self.users.add(i[0])
        for i in p:
            self.posts.add(i[0])
        
    def process_item(self, item, spider):
        if isinstance(item, postItem):
            post = item.get('pid')
            if post in self.posts:
                raise DropItem("Duplicate post found: %d" %(post))
            else:
                self.posts.add(post)
                return item
                
        if isinstance(item, userItem):
            user = item.get('uid')
            if user in self.users:
                raise DropItem("Duplicate user found: %d" %(user))
            else:
                self.users.add(user)
                return item     
            
            
            
            
