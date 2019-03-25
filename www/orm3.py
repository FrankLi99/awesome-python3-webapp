# -*- coding: utf-8 -*-
__author__ = 'Frank Li'

'''封装一个 ORM 操作 CRUD 模块'''
import asyncio
import logging;logging.basicConfig(level=logging.DEBUG)
import aiomysql

def log(sql,args):
    logging.info('SQL: {} , other args: {}'.format(sql,args))

@asyncio.coroutine
def create_pool(loop,**db_info):
    logging.info('start to create aiomysql database connection pool...')
    global __pool
    __pool = yield from aiomysql.create_pool(host=db_info.get('host','localhost'),
                                             port=db_info.get('port',3306),
                                             db=db_info.get('db'),
                                             user=db_info.get('user'),
                                             password=db_info.get('password'),
                                             charset=db_info.get('charset','utf8'),
                                             autocommit=db_info.get('autocommit',True),
                                             minsize=db_info.get('minsize',1),
                                             maxsize=db_info.get('maxsize',10),
                                             loop=loop)

@asyncio.coroutine
def select(sql,args,size=None):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        csr = yield from conn.cursor(aiomysql.DictCursor)
        yield from csr.execute(sql.replace('?','%s'),args or ())
        if size:
            rs = csr.fetchmany(size)
        else:
            rs = csr.fetchall()
        yield from csr.close()
        # logging.info('selected rows: {}'.format(len(rs)))
        return rs

@asyncio.coroutine
def execute(sql,args,autocommit=True):
    log(sql,args)
    global __pool
    with (yield from __pool) as conn:
        if not autocommit:
            yield from conn.begin()
        try:
            csr = yield from conn.cursor()
            yield from csr.execute(sql.replace('?','%s'),args or ())
            affectedRow = csr.rowcount
            logging.info('affected rows {}'.format(affectedRow))
            if not autocommit:
                yield from conn.commit()

        except BaseException as e:
            logging.error(e)
            raise

        finally:
            if not autocommit:
                yield from conn.close()


class Field(object):
    def __init__(self,name,column_type,is_pk,default):
        self.name = name
        self.column_type = column_type
        self.is_pk = is_pk
        self.default = default

    def __repr__(self):
        return '<{},{}:{}>'.format(self.__class__.__name__,self.column_type,self.name)
    __str__ = __repr__

# name , column_type,  default 是每一个 Field 子类都必须有的，而 is_pk 则不一定
class StringField(Field):
    def __init__(self,name=None,column_type='varchar(100)',is_pk=False,default=None):
        super(StringField,self).__init__(name,column_type,is_pk,default)

class IntegerField(Field):
    def __init__(self,name=None,column_type='bigint',is_pk=False,default=0):
        super(IntegerField.self).__init__(name,column_type,is_pk,default)

class FloatField(Field):
    def __init__(self,name=None,column_type='real',is_pk=False,default=0.0):
        super(FloatField,self).__init__(name,column_type,is_pk,default)

class BooleanField(Field):
    def __init__(self,name=None,column_type='boolean',default=False):
        super(BooleanField,self).__init__(name,column_type,False,default)

class TextField(Field):
    def __init__(self,name=None,column_type='text',default=None):
        super(TextField,self).__init__(name,column_type,False,default)

# 根据指定个数生成 占位符  ?,?
def create_args_string(num):
    if not isinstance(num,int):
        raise TypeError('the input param is not int!')
    return ','.join('?'*num)

# 定义 Model 的 类模板 ModelMetaclass 必须继承自 type ,此模板管理类属性，Model 本身实例方法管理实例属性
class ModelMetaclass(type):
    def __new__(cls,name,bases,attrs):
        if name=='Model':
            return type.__new__(cls,name,bases,attrs)

        tb_name = attrs.get('__table__',str.lower(name))
        mappings = {}
        fields = []
        primaryKey = None

        for k,v in attrs.items():
            if isinstance(v,Field):
                logging.info('found mapping {}<==>{}'.format(k,v))
                mappings[k] = v
                if v.is_pk:
                    if primaryKey:
                        raise RuntimeError('duplicated primary key at {}'.format(k))
                    primaryKey = k
                else:
                    fields.append(k)

        if not primaryKey:
            raise RuntimeError('the table {tb_name} has no primary key which is not allowed here...'.format(tb_name=tb_name))

        # 移除掉 类中与实例属性 同名的属性
        for k in mappings.keys():
            attrs.pop(k)

        escape_fields = ','.join(list(map(lambda f:'`{}`'.format(f),fields)))
        attrs['__mappings__'] = mappings
        attrs['__table__'] = tb_name
        attrs['__fields__'] = fields
        attrs['__primary_key__'] = primaryKey

        attrs['__select__'] = 'select {escape_fields},`{primaryKey}` from `{tb_name}`'.format(escape_fields=escape_fields,primaryKey=primaryKey,tb_name=tb_name)
        attrs['__insert__'] = 'insert into `{tb_name}`({escape_fields},`{primaryKey}`) values({args})'.format(tb_name=tb_name,escape_fields=escape_fields,primaryKey=primaryKey,args=create_args_string(len(fields)+1))
        attrs['__delete__'] = 'delete from `{tb_name}` where `{primaryKey}`=?'.format(tb_name=tb_name,primaryKey=primaryKey)
        attrs['__update__'] = 'update table `{tb_name}` set {set_cols} where {primaryKey}=?'.format(tb_name=tb_name,set_cols=''.join(map(lambda f:'`{}`'.format(f),fields)),primaryKey=primaryKey)

        return type.__new__(cls,name,bases,attrs)

# 开始为 User , Blog ， Comment 等编写 共性模板 ==》 Model 继承自 dict 方便 key 《==》 value 操作 ，元类采用上面定义好的 ModelMetaclass
class Model(dict,metaclass=ModelMetaclass):

    # 以下 __setattr__ , __getattr__ 都是为了 将 dict{key} 操作简化为 dict.key 操作
    def __setattr__(self, key, value):
        self[key] = value
    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"The object 'Model' has no attribute {}".format(key))
    def getValue(self,key):
        return getattr(self,key,None)
    def getValueOrDefault(self,key):
        value = self.getValue(key)
        if not value:
            # 如果没有给值，则使用默认值
            field = self.__mappings__[key] or None
            if field:
                value = field.default() if callable(field.default) else field.default
                logging.info('using default key: {} value:{}'.format(key,value))
                setattr(self,key,value)
        return value

    @classmethod
    @asyncio.coroutine
    def findAll(cls,where=None,args=None,**kw):

        select_sql = [cls.__select__]
        if where:
            select_sql.append('where')
            select_sql.append(where)
        if not args:
            args=[]

        orderBy = kw.get('orderBy',None)
        if orderBy:
            select_sql.append('order by')
            select_sql.append(orderBy)

        limit = kw.get('limit',None)
        if limit:
            select_sql.append('limit')
            if isinstance(limit,int):
                select_sql.append('?')
                args.append(limit)
            elif isinstance(limit,tuple):
                select_sql.append(create_args_string(2))
                args.extend(limit)
            else:
                raise ValueError('Invalid limit value {}.'.format(limit))

        rs = yield from select(''.join(select_sql),args)
        return [cls(**r) for r in rs]

    @classmethod
    @asyncio.coroutine
    def findNumber(cls,selectField,where=None,args=None):
        select_sql = ['select `{selectField}` as __num__ from `{tb_name}`'.format(selectField,tb_name=cls.__table__)]
        if where:
            select_sql.append('where')
            select_sql.append(where)
        rs = yield from select(''.join(select_sql),args,1)
        return rs[0]['__num__'] if len(rs) !=0 else None

    @classmethod
    @asyncio.coroutine
    def find(cls,pk):
        select_sql = '{selectList} where `{pk}`=?'.format(selectList=cls.__select__,pk=cls.__primary_key__)
        rs = yield from select(select_sql,[pk],1)
        return cls(**rs[0]) if len(list(rs))>0 else None

    # 实例方法
    @asyncio.coroutine
    def save(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__insert__,args)
        if rows!=1:
            logging.warning('failed insert into table {tb_name}, affected rows: {rows}'.format(tb_name=self.__table__,rows=rows))

    @asyncio.coroutine
    def remove(self):
        args = (self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__delete__,args)
        if rows!=1:
            logging.warining('failed to delete a row from table {tb_name}, affected rows: {rows}'.format(tb_name=self.__table__,rows=rows))

    @asyncio.coroutine
    def update(self):
        args = list(map(self.getValueOrDefault,self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = yield from execute(self.__update__,args)
        if rows!=1:
            logging.warning('failed to update table {tb_name}, affected rows {rows}'.format(tb_name=self.__table__,rows=rows))









