#include <iostream>
#include <string>
#include <vector>

size_t split(const std::string &str, const std::string &sep, std::vector<std::string> &result)
{
    // news...music.#.pop
    if (sep == "")
        return 0;
    size_t pos, idx = 0;
    while (idx < str.size())
    {
        pos = str.find(sep, idx);
        // 没找到
        if (pos == std::string::npos)
        {
            std::string tmp = str.substr(idx);
            result.push_back(tmp);
            return result.size();
        }
        // 找到了
        else if (idx == pos)
        { // 当前位置就是分隔符
            idx += sep.size();
            continue;
        }
        else
        {
            std::string tmp = str.substr(idx, pos - idx);
            idx = pos + sep.size();
            result.push_back(tmp);
        }
    }
    return result.size();
}

int main()
{
    std::string str = "..news...news...news......music.#.pop..";
    std::vector<std::string> arr;
    split(str, "", arr);
    for (auto str : arr)
        std::cout << str << std::endl;
    return 0;
}