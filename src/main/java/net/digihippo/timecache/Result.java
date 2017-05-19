package net.digihippo.timecache;

import java.util.function.Consumer;

abstract class Result<S, F>
{
    public abstract void consume(final Consumer<S> onSuccess, final Consumer<F> onFailure);

    static <S, F> Result<S, F> failure(F f)
    {
        return new Failure<>(f);
    }

    static <S, F> Result<S, F> success(S s)
    {
        return new Success<>(s);
    }

    private static final class Success<S, F> extends Result<S, F>
    {
        private final S s;

        private Success(S s)
        {
            this.s = s;
        }

        @Override
        public void consume(Consumer<S> onSuccess, Consumer<F> onFailure)
        {
            onSuccess.accept(s);
        }
    }

    private static final class Failure<S, F> extends Result<S, F>
    {
        private final F f;

        private Failure(F f)
        {
            this.f = f;
        }

        @Override
        public void consume(Consumer<S> onSuccess, Consumer<F> onFailure)
        {
            onFailure.accept(f);
        }
    }

    private Result() {}
}
