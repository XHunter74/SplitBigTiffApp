namespace SplitBigTiffApp;


public class Program
{
    public async static Task Main(string[] args)
    {
        if (args.Length == 0) return;
        await SplitAction.Split(args.First(), default);
    }
}