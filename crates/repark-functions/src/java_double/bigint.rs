use std::cmp::Ordering;
use std::sync::LazyLock;

const POW5_TABLE_LEN: usize = 346;
const POW5_LIMBS: usize = 26;

#[allow(clippy::cast_possible_truncation)]
fn low32(wide: u64) -> u32 {
    wide as u32
}

fn pow5_table() -> Box<[[u32; POW5_LIMBS]]> {
    let mut table = vec![[0u32; POW5_LIMBS]; POW5_TABLE_LEN].into_boxed_slice();
    table[0][0] = 1;
    let mut row = 1usize;
    while row < POW5_TABLE_LEN {
        let mut carry = 0u64;
        let mut limb = 0usize;
        while limb < POW5_LIMBS {
            let wide = u64::from(table[row - 1][limb]) * 5 + carry;
            table[row][limb] = low32(wide);
            carry = wide >> 32;
            limb += 1;
        }
        row += 1;
    }
    table
}

static POW5: LazyLock<Box<[[u32; POW5_LIMBS]]>> = LazyLock::new(pow5_table);

fn pow5_limbs(power: usize) -> Vec<u32> {
    if let Some(row) = POW5.get(power) {
        return row.to_vec();
    }
    let mut limbs = vec![1u32];
    for _ in 0..power {
        let mut carry = 0u64;
        for limb in &mut limbs {
            let wide = u64::from(*limb) * 5 + carry;
            *limb = low32(wide);
            carry = wide >> 32;
        }
        if carry != 0 {
            limbs.push(low32(carry));
        }
    }
    limbs
}

#[derive(Clone)]
pub(crate) struct FdBig {
    limbs: Vec<u32>,
}

impl FdBig {
    fn trimmed(mut limbs: Vec<u32>) -> Self {
        while limbs.last() == Some(&0) {
            limbs.pop();
        }
        Self { limbs }
    }

    pub(crate) fn is_zero(&self) -> bool {
        self.limbs.is_empty()
    }

    pub(crate) fn value_of_pow52(power5: usize, power2: usize) -> Self {
        let mut grown = Self::trimmed(pow5_limbs(power5));
        grown.left_shift(power2);
        grown
    }

    pub(crate) fn value_of_mul_pow52(value: u64, power5: usize, power2: usize) -> Self {
        let mut grown = Self::trimmed(vec![low32(value), low32(value >> 32)]);
        grown.mul_limbs(&pow5_limbs(power5));
        grown.trim();
        grown.left_shift(power2);
        grown
    }

    fn trim(&mut self) {
        while self.limbs.last() == Some(&0) {
            self.limbs.pop();
        }
    }

    fn mul_limbs(&mut self, factor: &[u32]) {
        if self.is_zero() {
            return;
        }
        let mut trimmed_factor = factor;
        while trimmed_factor.last() == Some(&0) {
            trimmed_factor = &trimmed_factor[..trimmed_factor.len() - 1];
        }
        if trimmed_factor.is_empty() {
            self.limbs.clear();
            return;
        }
        let mut product = vec![0u32; self.limbs.len() + trimmed_factor.len()];
        for (index, limb) in self.limbs.iter().enumerate() {
            let mut carry = 0u64;
            for (slot, other) in trimmed_factor.iter().enumerate() {
                let wide =
                    u64::from(product[index + slot]) + u64::from(*limb) * u64::from(*other) + carry;
                product[index + slot] = low32(wide);
                carry = wide >> 32;
            }
            let mut slot = index + trimmed_factor.len();
            while carry != 0 {
                let wide = u64::from(product[slot]) + carry;
                product[slot] = low32(wide);
                carry = wide >> 32;
                slot += 1;
            }
        }
        self.limbs = product;
    }

    fn mul_small(&mut self, factor: u32) -> u32 {
        let mut carry = 0u64;
        for limb in &mut self.limbs {
            let wide = u64::from(*limb) * u64::from(factor) + carry;
            *limb = low32(wide);
            carry = wide >> 32;
        }
        low32(carry)
    }

    pub(crate) fn mult_by_10(&mut self) {
        if self.is_zero() {
            return;
        }
        let carry = self.mul_small(10);
        if carry != 0 {
            self.limbs.push(carry);
        }
    }

    pub(crate) fn left_shift(&mut self, shift: usize) {
        if self.is_zero() || shift == 0 {
            return;
        }
        let mut grown = vec![0u32; shift >> 5];
        grown.extend_from_slice(&self.limbs);
        self.limbs = grown;
        let bits = shift & 31;
        if bits == 0 {
            return;
        }
        let mut carry = 0u32;
        for limb in &mut self.limbs {
            let next = *limb >> (32 - bits);
            *limb = (*limb << bits) | carry;
            carry = next;
        }
        if carry != 0 {
            self.limbs.push(carry);
        }
    }

    pub(crate) fn normalization_bias(&self) -> usize {
        let zeros = self.limbs.last().unwrap_or(&0).leading_zeros() as usize;
        if zeros < 4 { 28 + zeros } else { zeros - 4 }
    }

    pub(crate) fn cmp(&self, other: &Self) -> Ordering {
        match self.limbs.len().cmp(&other.limbs.len()) {
            Ordering::Equal => {}
            settled => return settled,
        }
        for index in (0..self.limbs.len()).rev() {
            if self.limbs[index] != other.limbs[index] {
                return self.limbs[index].cmp(&other.limbs[index]);
            }
        }
        Ordering::Equal
    }

    pub(crate) fn add_and_cmp(&self, left: &Self, right: &Self) -> Ordering {
        let width = left.limbs.len().max(right.limbs.len()) + 1;
        let mut sum = vec![0u32; width];
        let mut carry = 0u64;
        for ((slot, left_limb), right_limb) in sum
            .iter_mut()
            .zip(left.limbs.iter().chain(std::iter::repeat(&0u32)))
            .zip(right.limbs.iter().chain(std::iter::repeat(&0u32)))
        {
            let wide = u64::from(*left_limb) + u64::from(*right_limb) + carry;
            *slot = low32(wide);
            carry = wide >> 32;
        }
        self.cmp(&Self::trimmed(sum))
    }

    #[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
    fn subtract(&mut self, subtrahend: &Self) {
        if self.limbs.len() < subtrahend.limbs.len() {
            self.limbs.resize(subtrahend.limbs.len(), 0);
        }
        let mut borrow = 0i64;
        for index in 0..self.limbs.len() {
            let take = i64::from(*subtrahend.limbs.get(index).unwrap_or(&0)) + borrow;
            let wide = i64::from(self.limbs[index]) - take;
            self.limbs[index] = wide as u32;
            borrow = i64::from(wide < 0);
        }
    }

    pub(crate) fn quo_rem_iteration(&mut self, divisor: &Self) -> u32 {
        if self.cmp(divisor) == Ordering::Less {
            self.mult_by_10();
            return 0;
        }
        let top = divisor.limbs.last().unwrap_or(&0).max(&1);
        let mut quotient = self.limbs.last().unwrap_or(&0) / top;
        loop {
            let mut probe = divisor.clone();
            let carry = probe.mul_small(quotient);
            if carry == 0 && probe.cmp(self) != Ordering::Greater {
                self.subtract(&probe);
                break;
            }
            if quotient == 0 {
                break;
            }
            quotient -= 1;
        }
        self.trim();
        self.mult_by_10();
        quotient
    }
}
