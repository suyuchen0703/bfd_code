import re

def add_quotes_to_english(text):
    """
    给包含英文字母的内容添加双引号 - 最终稳定版本
    title:作为特殊标识不加双引号
    """
    # 定义运算符集合
    operators = {'|', '<', '>', '~', '&', ':', '(', ')'}
    
    # 如果整个文本都是英文字母、数字或空格，直接加引号
    if re.match(r'^[a-zA-Z0-9\s]+$', text.strip()):
        return f'"{text}"'
    
    # 检查是否包含英文字母
    if not re.search(r'[a-zA-Z]', text):
        return text
    
    # 如果不包含运算符，整个加引号
    if not any(op in text for op in operators):
        return f'"{text}"'
    
    # 核心算法：逐字符扫描，识别英文单词块
    result = []
    i = 0
    text_len = len(text)
    
    while i < text_len:
        # 检查是否是title:特殊标识
        if text[i:i+6] == 'title:':
            result.append('title:')  # 不加引号
            i += 6
        # 如果当前字符是运算符
        elif text[i] in operators:
            result.append(text[i])
            i += 1
        else:
            # 开始收集非运算符字符块
            start = i
            while i < text_len and text[i] not in operators and text[i:i+6] != 'title:':
                i += 1
            
            # 获取这个字符块
            chunk = text[start:i]
            if chunk and re.search(r'[a-zA-Z]', chunk):
                # 包含英文字母，加引号
                result.append(f'"{chunk}"')
            else:
                # 不包含英文字母，保持原样
                result.append(chunk)
    
    return ''.join(result)

def process_keywords_file(input_file, output_file):
    """
    处理关键词文件，给英文内容加双引号
    """
    try:
        # 读取文件内容
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 直接对整个内容进行处理
        result = add_quotes_to_english(content)
        
        # 写入结果文件
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(result)
        
        print(f"处理完成！结果已保存到 {output_file}")
        return True
        
    except Exception as e:
        print(f"处理过程中出现错误: {e}")
        return False

def main():
    input_file = r'E:\document\python\百分点\2026\2月\2.12\关键词.txt'
    output_file = r'E:\document\python\百分点\2026\2月\2.12\关键词_加引号后.txt'
    
    print("开始处理关键词文件...")
    success = process_keywords_file(input_file, output_file)
    
    if success:
        print("处理成功完成！")
    else:
        print("处理失败！")

if __name__ == "__main__":
    main()