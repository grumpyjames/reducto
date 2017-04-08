package net.digihippo.timecache;

public class ClassLoading
{
    static <S> Result<S, Exception> loadAndCast(final String className, final Class<S> klass)
    {
        try
        {
            Class<?> factoryClass = Class.forName(className);
            return Result.success(klass.cast(factoryClass.getConstructor().newInstance()));
        }
        catch (Exception e)
        {
            return Result.failure(e);
        }
    }
}
