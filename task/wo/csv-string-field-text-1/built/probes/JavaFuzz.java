import java.io.*;
import java.util.*;

public class JavaFuzz {
    public static void main(String[] a) throws Exception {
        int[] alphabet = {0x391, 0x3a3, 0x3a3, 0x3a3, 0x392, 'a', '1', '-', '.', ',', '\'', '"', '_', ' ', 0x4e2d, 0x3400, 0xac00,
            0x301, 0x345, 0x200b, 0xad, 0x10400, 0x10000, 0x1f600, 0x1d7ce, '$', '%', '#', 0x2160, 0x964, 0x2027, 0x600, 0x3b6,
            0x1c5, 0x2b0, 0xaa, 0x30a1, 0x30fc, 0x309b, 0x3099, 0x24b6, 0x1d400, 0x10428, 0xe0020, 0x20000, 0x66b, 0x903, 0x3c3, 0x3c2, 0x130, '\t', '\n'};
        Random r = new Random(Long.parseLong(a[1]));
        int n = Integer.parseInt(a[2]);
        try (PrintWriter w = new PrintWriter(new BufferedWriter(new FileWriter(a[0])))) {
            for (int k = 0; k < n; k++) {
                int len = 1 + r.nextInt(9);
                StringBuilder b = new StringBuilder();
                StringBuilder hex = new StringBuilder();
                for (int i = 0; i < len; i++) {
                    int cp = alphabet[r.nextInt(alphabet.length)];
                    b.appendCodePoint(cp);
                    hex.append(Integer.toHexString(cp)).append(' ');
                }
                StringBuilder sig = new StringBuilder();
                b.toString().toLowerCase().codePoints().forEach(cp -> sig.append(Integer.toHexString(cp)).append(' '));
                w.println(hex.toString().trim() + "\t" + sig.toString().trim());
            }
        }
    }
}
