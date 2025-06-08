from functools import cached_property
from typing import Any, Callable, Generic, TypeVar

from pydantic import BaseModel, TypeAdapter
from sqlalchemy import Dialect, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import JSON, TypeDecorator, TypeEngine

ModelT = TypeVar("ModelT", bound=BaseModel)


class BasePydanticType(TypeDecorator[ModelT], Generic[ModelT]):
    impl: TypeEngine[Any] | type[TypeEngine[Any]]
    cache_ok: bool | None

    _pydantic_model_type: type[ModelT]
    _serializer: Callable[[ModelT], Any]
    _deserializer: Callable[[Any | None], ModelT]

    def __init__(
        self,
        pydantic_model_type: type[ModelT],
        *args: Any,
        serializer: Callable[[ModelT], Any] | None = None,
        deserializer: Callable[[Any], ModelT] | None = None,
        **kwargs: Any,
    ):
        super().__init__(*args, **kwargs)

        self._pydantic_model_type = pydantic_model_type

        self._serializer = serializer or self._default_serializer
        self._deserializer = deserializer or self._default_deserializer

    @cached_property
    def type_adapter(self) -> TypeAdapter[ModelT]:
        return TypeAdapter(self._pydantic_model_type)

    def _default_serializer(self, model: ModelT) -> Any:
        return model.model_dump(mode="json")

    def _default_deserializer(self, value: Any | None) -> ModelT:
        return self.type_adapter.validate_python(value)

    # FIXME: thats trashy, alembic `render_item` needs to know type from `sqlalchemy.dialects` to render import properly
    # this and `load_dialect_impl` was added just to handle cases when we want to use different `impl` depending on dialect in one class;
    # for example `impl=JSON` but if its postgres we want to use `impl=JSONB`
    # maybe all of this is pointless and we should create second class with `impl=JSONB` if its postgres and second class with `impl=JSON` for rest?
    #
    # couldn't figure out more elegant/fitting way to do it
    def get_dialect_type_impl(
        self, dialect: Dialect
    ) -> TypeEngine[Any] | type[TypeEngine[Any]]:
        """Return the public-facing SQLAlchemy type for a given dialect name.

        This method serves as a hook for subclasses to provide dialect-specific
        type implementations. The base implementation returns the generic `self.impl`.

        Subclasses should override this method to handle specific database backends.
        For example, to use PostgreSQL's `JSONB` type when the dialect is
        'postgresql', you would implement it as follows:

        .. code-block:: python

            from sqlalchemy.dialects import postgresql

            def __get_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any] | type[TypeEngine[Any]]:
                if dialect.name == "postgresql":
                    return postgresql.JSONB
                return super().__get_dialect_impl(dialect)

        Args:
            dialect: The name of the dialect (e.g., 'postgresql', 'sqlite').

        Returns:
            The SQLAlchemy type class to use for this dialect.
        """
        return self.impl

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        impl = self.get_dialect_type_impl(dialect)
        return dialect.type_descriptor(impl() if callable(impl) else impl)

    def process_bind_param(
        self,
        value: ModelT | None,
        dialect: Dialect,
    ) -> Any:
        if value is None:
            return None

        return self._serializer(value)

    def process_result_value(
        self,
        value: Any | None,
        dialect: Dialect,
    ) -> ModelT | None:
        if value is None:
            return None

        return self._deserializer(value)


class PydanticJSON(BasePydanticType[ModelT]):
    impl = JSON
    cache_ok = True

    def get_dialect_type_impl(
        self, dialect: Dialect
    ) -> TypeEngine[Any] | type[TypeEngine[Any]]:
        if dialect.name == "postgresql":
            return JSONB
        return self.impl


class PydanticString(BasePydanticType[ModelT]):
    impl = String
    cache_ok = True

    def _default_serializer(self, model: ModelT) -> Any:
        return model.model_dump_json()

    def _default_deserializer(self, value: Any):
        return self._pydantic_model_type.model_validate_json(value)


class UserMeta(BaseModel):
    a: str
    b: int
    c: bool | None


class Base(DeclarativeBase): ...


class UserJson(Base):
    __tablename__ = "users_json"

    id: Mapped[int] = mapped_column(primary_key=True)
    meta: Mapped[UserMeta] = mapped_column(PydanticJSON(UserMeta))


class UserString(Base):
    __tablename__ = "users_string"

    id: Mapped[int] = mapped_column(primary_key=True)
    meta: Mapped[UserMeta] = mapped_column(PydanticString(UserMeta))


def main():
    from sqlalchemy import create_engine, select
    from sqlalchemy.orm import Session

    engine = create_engine("sqlite:///database.db", echo=True)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    s = Session(engine)
    u1 = UserJson(meta=UserMeta(a="test", b=1, c=True))
    u2 = UserString(meta=UserMeta(a="test", b=1, c=None))

    s.add(u1)
    s.add(u2)
    s.commit()

    q1 = s.execute(select(UserJson)).scalars().first()
    assert q1 is not None
    assert q1.meta

    q2 = s.execute(select(UserString)).scalars().first()
    assert q2 is not None
    assert q2.meta

    print(f"{q1.meta=}")
    print(f"{q2.meta=}")
    breakpoint()


if __name__ == "__main__":
    main()
