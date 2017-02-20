# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from scrapy.exceptions import DropItem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, ForeignKey
from sqlalchemy.types import Integer, String, DateTime, Text, TIMESTAMP
from sqlalchemy.sql import func
from sqlalchemy.dialects.mysql import INTEGER
from sqlalchemy.ext.declarative import declarative_base
from BiggerPockets.items import postItem, userItem
from sqlalchemy.exc import InvalidRequestError

Base = declarative_base()
class Posts(Base):
    __tablename__ = 'forumposts'
    URL = Column(String(200), primary_key=True)
    replyid = Column(Integer, primary_key=True)
    pid = Column(Integer) # post id    
    title = Column(String(500))
    category = Column(String(500)) # discussion category
    categoryURL = Column(String(500))
    uid = Column(Integer, ForeignKey('forumusers.uid', onupdate="CASCADE", ondelete='CASCADE')) # user id
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
    crawl_time = Column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

class BiggerpocketsPipeline(object):
    def open_spider(self, spider):
        connStr = 'mysql+mysqldb://root:home123@127.0.0.1/homeDB'
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
        post = Posts(URL=item.get('URL'),
                     replyid=item.get('replyid'),
                     pid=item.get('pid'),
                     title=item.get('title'),
                     category=item.get('category'),
                     categoryURL=item.get('categoryURL'),
                     uid=item.get('uid'),
                     replyTo=item.get('replyTo'),
                     postTime=item.get('postTime'),
                     body=item.get('body'),
        )
        # Avoid a post was deleted in the future
        while True:
            try:
                self.session.add(post)
                self.session.commit()
                break
            except InvalidRequestError:
                self.session.rollback()
                post.replyid += 1
            else:
                raise DropItem('Invalid item found...')
                break
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
        connStr = 'mysql+mysqldb://root:home123@127.0.0.1/homeDB'
        self.engine = create_engine(connStr, convert_unicode=True, echo=False)
        self.DB_session = sessionmaker(bind=self.engine)
        Base.metadata.create_all(self.engine)
        self.session = self.DB_session()
        self.users = set()
        self.users_seen = set()
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
            if user in self.users_seen:
                raise DropItem("Duplicate user found: %d" %(user))
            elif user in self.users:
                d = {'colleagues': item.get('colleagues'),
                     'followers': item.get('followers'),
                     'following': item.get('following'),
                     'numPosts': item.get('numPosts'),
                     'numVotes': item.get('numVotes'),
                     'numAwards': item.get('numAwards'),
                     'account': item.get('account'),
                     'city': item.get('city'),
                     'state': item.get('state'),                            
                }
                self.session.query(Users).filter(Users.uid == user).\
                    update(d)
                self.session.commit()     
                self.users_seen.add(user)
                raise DropItem("Updating user: %d" %(user))
            else:    
                self.users_seen.add(user)
                return item     
            
            
            
            
