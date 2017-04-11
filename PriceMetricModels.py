import datetime
from sqlalchemy import (Column, 
                        String, 
                        Integer, 
                        Boolean, 
                        Numeric, 
                        DateTime,
                        ForeignKey
                        )
from sqlalchemy.ext.declarative import declarative_base



# create base class.
Base = declarative_base()


class Manufacturers(Base):
    
    __tablename__ = 'manufacturers'
    
    manufacturer_id = Column(Integer, primary_key=True)
    name = Column(String(250), unique=True)
    description = Column(String(250), nullable=True)
    
    def __repr__(self):
        return "Manufacturer Name: %s" % self.name
    
    
class Sellers(Base):
    
    __tablename__ = 'sellers'
    
    seller_id = Column(Integer, primary_key=True)
    name = Column(String(250), unique=True)
    description = Column(String(550), nullable=True)
    trusted_store = Column(Integer, default=0)
    seller_rating = Column(String(20), nullable=True)
    reviews = Column(String(50), nullable=True)
    website = Column(String(250), nullable=True)    
    
    def __repr__(self):
        return "Sellers Name: %s" % self.name
    
    
class Products(Base):
     
    __tablename__ = 'products'
    
    product_id = Column(Integer, primary_key=True)
    manufacturer_id = Column(Integer, ForeignKey('manufacturers.manufacturer_id'))
    name = Column(String(250), unique=True)
    product_store_link = Column(String(1000), nullable=True)
    part_number = Column(String(50), nullable=False)
    
    def __repr__(self):
        return self.part_number


class SellerProducts(Base):
    
    __tablename__ = 'seller_products'
    
    seller_product_id = Column(Integer, primary_key=True)
    seller_id = Column(Integer, ForeignKey('sellers.seller_id'))
    product_id = Column(Integer, ForeignKey('products.product_id'))
    product_link = Column(String(550), nullable=True)
    

class PriceMetrics(Base):
    
    __tablename__ = 'price_metrics'
    
    price_metric_id = Column(Integer, primary_key=True)
    seller_product_id = Column(Integer, ForeignKey('seller_products.seller_product_id'))
    base_price = Column(Numeric)
    tax = Column(Numeric)
    shipping_fee = Column(Numeric)
    total_price = Column(Numeric)
    datetime = Column(DateTime, default = datetime.datetime.now())
        

class UnstablePartNums(Base):
    '''
    This table used for checking part number correciton, 
    if the succ_times > 5 and succ_times divides failed_times > 5, the part number will move into products table
    else if the failed_times > 5 and failed_times divides succ_times > 5, the part number will move into failed_part_nums table.
    '''
    __tablename__ = 'unstable_part_nums'
    
    unstable_part_num_id = Column(Integer, primary_key=True)
    part_num = Column(String(50), nullable=False)
    failed_times = Column(Integer, nullable=False, default=0)
    insert_datetime = Column(DateTime, nullable=False, default=datetime.datetime.now())
    is_move_to_stable = Column(Boolean, nullable=False, default=0)
    is_move_to_failed = Column(Boolean, nullable=False, default=0)
    move_datetime = Column(DateTime, nullable=True )
    
    
class SearchEngines(Base):
    
    __tablename__ = 'search_engines'
    
    search_engine_id = Column(Integer, primary_key=True)
    search_engine = Column(String(50), nullable=False)
    description = Column(String(550), nullable=True)
    
    
class FailedPartNums(Base):
    
    __tablename__ = 'failed_part_nums'
    
    failed_part_num_id = Column(Integer, primary_key=True)
    search_engine_id = Column(Integer, ForeignKey('search_engines.search_engine_id'))
    part_num = Column(String(50), nullable=False)
    create_datetime = Column(DateTime, default=datetime.datetime.now())


class ProductStatus(Base):
    
    __tablename__ = 'product_status'
    
    product_status_id = Column(Integer, primary_key=True)
    product_id = Column(Integer, ForeignKey('products.product_id'))
    search_engine_id = Column(Integer, ForeignKey('search_engines.search_engine_id'))
    failed_times = Column(Integer, nullable=False, default=0)
    last_failed_datetime = Column(DateTime, default=datetime.datetime.now(), nullable=True)
    

# def get_or_create(model, defaults={}, **kwargs): 
#     session = Session()
#     query = session.query(model).filter_by(**kwargs)
#     instance = query.first()
#     if instance:
#         return instance, False
#     else:
#         try:
#             if session.autocommit:
#                 session.begin() # begin
#             else:
#                 session.begin(nested=True) # savepoint
#             params = dict((k, v) for k, v in kwargs.iteritems() if not isinstance(v, ClauseElement))
#             params.update(defaults)
#             instance = model(**params)
#             session.add(instance)
#             if session.autocommit:
#                 session.commit() # release savepoint
#             else:
#                 session.commit() # release savepoint
#                 session.commit() # commit
#             return instance, True
#         except Exception, e:
#             session.rollback() # rollback or rollback to savepoint
#             instance = query.one()
#             return instance, False
