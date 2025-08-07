from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from config import DB_HOST, DB_NAME, DB_PASS, DB_PORT, DB_USER
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Text, Integer, Float, DateTime, Date, Column

Base = declarative_base()
DATABASE_URL = f"postgresql+asyncpg://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


class spimex_trading_results(Base):
    __tablename__ = "spimex_trading_results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    exchange_product_id = Column(Text)
    exchange_product_name = Column(Text)
    oil_id = Column(Text)
    delivery_basis_id = Column(Text)
    delivery_basis_name = Column(Text)
    delivery_type_id = Column(Text)
    volume = Column(Float)
    total = Column(Integer)
    count = Column(Integer)
    date = Column(Date)
    created_on = Column(DateTime)
    updated_on = Column(DateTime)


def get_engine_and_session():
    engine = create_async_engine(DATABASE_URL, pool_pre_ping=True)
    Session = async_sessionmaker(bind=engine)
    return engine, Session


async def create_table(engine) -> bool:
    try:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    except Exception as e:
        print(str(e))
        print("БД уже существует")
        return False
    print("БД успешно создана")
    return True


async def async_insert_to_db(obj: spimex_trading_results, session):
    session.add(obj)
    await session.commit()
